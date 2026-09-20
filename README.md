# Pipeline de e-commerce em larga escala

Este é um protótipo de ponta a ponta pra um cenário clássico de e-commerce em alta escala: chega um webhook de pedido, esse pedido precisa ser gravado sem duplicar mesmo sob concorrência pesada, o histórico de endereço e status não pode se perder, e quando um pedido é aprovado um ERP externo (fictício, aqui) precisa ser notificado sem depender de polling. Além disso tem uma carga analítica de 10 milhões de linhas pra popular uma tabela de histórico.

O projeto nasceu de um desafio técnico de nível sênior, mas ficou como material de estudo/portfólio: dá pra usar como referência de como lidar com idempotência, concorrência, modelagem com auditoria histórica e carga em massa no SQL Server.

## Como o pipeline funciona

```
                     ┌────────────────────┐
  webhook parceiro → │  webhook-receiver   │  (FastAPI, sem acesso a DB)
                     │  valida + publica   │
                     └─────────┬───────────┘
                               │  produce
                               ▼
                     ┌────────────────────┐
                     │  Redpanda (Kafka)  │  topic: webhook-events
                     └─────────┬───────────┘
                               │  consume em lote
                               ▼
                     ┌────────────────────┐        ┌───────────────┐
                     │ ingestion-consumer │──MERGE→│  SQL Server   │
                     │ idempotente        │        │  (ecommerce)  │
                     └─────────┬───────────┘        └───────┬───────┘
                               │ status = APPROVED           │
                               ▼                              │ Airflow (LocalExecutor)
                     ┌────────────────────┐                   │ DAG bulk_load_dimensions
                     │ topic: order-approved                  │ 10M+ linhas, bcp+TABLOCK
                     └─────────┬───────────┘                   ▼
                               ▼                       order_history_fact
                     ┌────────────────────┐
                     │ order-approved-    │  POST + retry (tenacity)
                     │ worker             │──────────────► mock-external-api (ERP fictício)
                     └────────────────────┘
                               │ falhas esgotadas
                               ▼
                     topic: order-approved-dlq
```

O webhook chega numa API HTTP fina (`webhook-receiver`) que só valida e publica no Kafka — nunca toca o banco. Um consumer (`ingestion-consumer`) lê esses eventos, grava tudo no SQL Server dentro de uma transação idempotente, e quando um pedido vira `APPROVED` publica em outro tópico. Um worker separado (`order-approved-worker`) escuta esse tópico e entrega o pedido pro ERP externo, com retry. Em paralelo, uma DAG do Airflow carrega uma tabela analítica de histórico com milhões de linhas sintéticas, sem relação com o fluxo transacional.

## Estrutura do projeto

```
.
├── common/                  # código compartilhado entre serviços (só a fábrica de conexão de banco, hoje)
├── db/                      # schema (SQLAlchemy), migrations (Alembic) e o container que as aplica
│   ├── models.py
│   └── migrations/versions/
├── airflow/                 # DAGs (carga de 10M linhas + limpeza do log) e o gerador de dados sintéticos
│   ├── dags/bulk_load_dimensions.py
│   ├── dags/pipeline_log_retention.py
│   └── scripts/
├── services/
│   ├── webhook_receiver/    # borda HTTP: recebe o webhook, publica no Kafka, não fala com o banco
│   ├── ingestion_consumer/  # consumer idempotente: Kafka -> SQL Server
│   ├── order_approved_worker/  # consome pedidos aprovados e notifica o ERP externo
│   ├── mock_external_api/   # ERP fictício, com falha configurável pra testar o retry
│   └── load_simulator/      # dispara uma carga de webhooks pra testar idempotência/concorrência
├── monitoring/
│   ├── prometheus.yml                # o que o Prometheus coleta e de onde
│   └── grafana/
│       ├── provisioning/             # datasource do Prometheus + o provider do dashboard
│       └── dashboards/pipeline.json  # o dashboard em si, carregado automaticamente
├── docker-compose.yml
├── pyproject.toml           # config do ruff (lint) e mypy (checagem de tipos)
└── .env.example
```

Cada serviço tem seu próprio `Dockerfile` e `requirements.txt` — só o `ingestion-consumer` importa `common/`, porque é o único que precisa da fábrica de conexão com o SQL Server compartilhada com o schema (`db/models.py`).

## Antes de rodar

Você precisa de:

- Docker e Docker Compose v2 (`docker compose`, não `docker-compose`)
- Pelo menos ~8-10 GB de RAM livres — o SQL Server sozinho já reserva 2 GB, e o resto da stack (Redpanda, Postgres do Airflow, scheduler, webserver, 4 serviços Python, Prometheus, Grafana, cAdvisor, o exporter do SQL Server) roda tudo junto
- Portas livres: `1433` (SQL Server), `8000` (webhook-receiver), `8080` (Airflow), `9000` (ERP fictício), `9092`/`9644` (Redpanda), `9090` (Prometheus), `3000` (Grafana)

## Subindo o ambiente

```bash
cp .env.example .env   # dá pra trocar a senha do SA se quiser
docker compose up -d --build
```

Só isso. Um comando sobe a stack inteira — SQL Server, Redpanda, os quatro serviços Python e o Airflow completo (Postgres de metadados, init, webserver e scheduler) — na ordem certa, sozinho. Isso funciona porque cada serviço declara no `docker-compose.yml` do que ele depende e em que condição (`condition: service_healthy` ou `service_completed_successfully`): o `migrate` só roda depois que o SQL Server responde ao healthcheck, o `ingestion-consumer` só sobe depois que o `migrate` e o `topics-init` terminam com sucesso, e assim por diante. Você não precisa orquestrar isso na mão.

`docker compose up -d --build` sozinho não sobe o `load-simulator` — ele fica fora de propósito (tem `profiles: ["tools"]` no compose), porque é uma ferramenta de teste que você dispara sob demanda, não um serviço de fundo.

Na primeira subida, o SQL Server pode levar entre 30 segundos e um par de minutos pra ficar pronto (é uma imagem pesada) — os serviços que dependem dele esperam automaticamente, então não é preciso reagir a isso, só ter paciência. Se quiser acompanhar o progresso:

```bash
docker compose ps        # mostra o status/healthcheck de cada serviço
docker compose logs -f sqlserver
```

O Airflow fica em `http://localhost:8080` (login `admin`/`admin`, a menos que você tenha mudado no `.env`).

Se algo der errado e você quiser isolar em qual etapa travou, dá pra subir por partes, na ordem que o compose já respeitaria sozinho:

```bash
docker compose up -d --build sqlserver redpanda topics-init
docker compose up --build migrate
docker compose up -d --build webhook-receiver ingestion-consumer \
    order-approved-worker mock-external-api
docker compose up -d --build airflow-postgres airflow-init airflow-webserver airflow-scheduler
```

## Testando a ingestão: idempotência e concorrência

O `load-simulator` dispara uma leva de pedidos simulados, incluindo uma fração deles duplicada de propósito, pra provar que replay de webhook não duplica nada:

```bash
docker compose --profile tools run --rm load-simulator \
    python simulate.py --url http://webhook-receiver:8000/webhooks/events \
    --orders 2000 --concurrency 200 --duplicate-rate 0.15
```

Depois confira no SQL Server:

```sql
SELECT COUNT(*) FROM orders;                    -- deve bater com --orders
SELECT COUNT(*) FROM processed_webhook_events;  -- menor que o total de requests
                                                 -- (as duplicatas viram no-op)
```

Pra ver o consumer escalando horizontalmente:

```bash
docker compose up -d --scale ingestion-consumer=3
```

O Redpanda rebalanceia as partições do tópico `webhook-events` entre as réplicas automaticamente. Cada partição pertence a um único consumer por vez, então não tem risco de duas réplicas processarem o mesmo pedido ao mesmo tempo.

Pra confirmar que um pedido aprovado realmente chegou no ERP fictício:

```bash
curl http://localhost:9000/erp/orders/_debug
```

## Rodando a carga de 10M linhas

Dispare a DAG `bulk_load_dimensions` pela interface do Airflow, ou via CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger bulk_load_dimensions
```

Antes de rodar os 10 milhões completos, vale um teste rápido:

```bash
docker compose exec airflow-scheduler airflow dags trigger bulk_load_dimensions \
    --conf '{"total_rows": 100000, "batch_size": 10000}'
```

## Desligando tudo

```bash
docker compose down          # mantém os volumes (dados do SQL Server, Redpanda, Airflow)
docker compose down -v       # apaga tudo, começa do zero na próxima subida
```

## Por que Kafka e não só uma API que grava direto no banco

O jeito mais simples de resolver "recebe um webhook e grava num banco" seria uma API HTTP que já processa e grava dentro da própria requisição. Isso não escala bem quando o volume esperado é de milhares de req/s: a API precisaria implementar, ela mesma, uma fila interna pra não estourar o pool de conexões do banco — ou seja, reinventaria o que um message broker já resolve.

Com Kafka (aqui, Redpanda — mesmo protocolo, um único binário, sem depender de ZooKeeper) no meio:

- O webhook termina assim que o evento é publicado no tópico. Rápido e desacoplado da velocidade do banco.
- Backpressure vem de graça: se o Redpanda estiver lento, a publicação demora mais, o que naturalmente segura quem está chamando — sem fila descontrolada crescendo dentro do processo.
- A notificação do ERP externo (a parte "orientada a evento" do pipeline) sai natural: o consumer publica `order-approved` no instante em que o status muda, sem qualquer polling no banco.
- Escalar consumidores é nativo — mais réplicas no mesmo consumer group, e o Redpanda rebalanceia as partições sozinho.

O trade-off é real: mais uma peça de infraestrutura, e uma janela pequena "at-least-once" entre o consumer commitar a transação no banco e commitar o offset do Kafka. Isso é mitigado processando tudo de forma idempotente — reprocessar um evento já visto é um no-op.

## Idempotência e concorrência

- **Idempotência**: a tabela `processed_webhook_events` (chave primária = `event_id` do parceiro) é gravada na mesma transação das gravações de negócio. Se o `event_id` já existe, a inserção viola a chave primária, a transação inteira é descartada e o evento é tratado como já processado — sem duplicar pedido, pagamento ou nota fiscal.
- **Conexões limitadas**: o pool de conexões do consumer é pequeno e limitado (`common/db.py`) — não importa quantas partições do Kafka existam, o número de conexões simultâneas contra o SQL Server é sempre travado por processo.
- **Transações curtas**: cada evento é processado numa transação isolada, sem locks de longa duração esperando I/O externo.
- **READ_COMMITTED_SNAPSHOT** habilitado no banco (`db/entrypoint.sh`): leituras concorrentes usam uma versão consistente dos dados em vez de ficarem bloqueadas atrás de quem está escrevendo. Isso reduz bem mais o bloqueio leitor/escritor do que só limitar conexões.
- Os offsets do Kafka só são commitados depois que a transação no SQL Server já commitou. Se o processo cair no meio de um lote, as mensagens não commitadas voltam a ser entregues — seguro, porque reprocessar é idempotente.

## Resiliência: o que acontece quando algo quebra

- **Container caiu, sobe sozinho.** Todo serviço de longa duração no `docker-compose.yml` tem `restart: unless-stopped` — se o processo morrer por qualquer motivo (OOM, uma exceção não tratada, o host reiniciar), o Docker sobe de novo sozinho, sem precisar de `docker compose up` manual. Containers de execução única (`migrate`, `topics-init`, `airflow-init`) ficam de fora de propósito — eles têm que rodar até o fim e parar, não ficar reiniciando em loop.
- **Query travada numa lock falha rápido, não trava pra sempre.** Toda conexão com o SQL Server (`ingestion-consumer` e a DAG do Airflow) seta `LOCK_TIMEOUT` pra 10 segundos assim que conecta. Sem isso, uma query bloqueada por outra transação concorrente esperaria indefinidamente — nesse tempo, a thread que processa aquela mensagem fica presa, e se isso se repetir o suficiente, esgota as threads disponíveis e trava a ingestão inteira. Com o timeout, vira um erro (`OperationalError`) que o código sabe tratar.
- **Erro transitório de banco tem retentativa; erro de dado, não.** `ingestion-consumer` distingue as duas coisas: um deadlock ou lock timeout (`OperationalError`) é retentado até 3 vezes com backoff antes de desistir — a chance real de a mesma transação passar na segunda tentativa é boa, porque geralmente é só concorrência momentânea. Já um dado truncado (campo maior que a coluna) ou uma escrita rejeitada pelo gatilho de pedido imutável são erros permanentes — retentar não muda o resultado, então são só logados e a mensagem é descartada, sem gastar tempo tentando de novo à toa.
- **Uma mensagem ruim não derruba o consumer inteiro.** JSON malformado ou um payload sem um campo esperado é capturado, logado com o offset da mensagem, e pulado — o resto do tópico continua sendo processado normalmente. Sem isso, uma mensagem mal formada tiraria a ingestão de todas as partições até alguém reiniciar o serviço na mão.
- **A carga de 10M linhas não fica pendurada pra sempre.** O `bcp` roda com um timeout (`BCP_TIMEOUT_SECONDS`, padrão 2h — bem acima dos ~55min medidos) — se a rede cair no meio da transferência, a task falha em vez de travar o worker do Airflow indefinidamente, e a retentativa já configurada na DAG (`retries: 1`) assume dali. A DAG também tem `max_active_runs=1`: não dá pra disparar duas cargas de 10M ao mesmo tempo brigando pelo mesmo `TABLOCK`.
- **Um retry da carga de 10M não duplica nem deixa lixo pra trás.** O `load_batch_id` de cada execução é derivado do `run_id` da DAG (estável entre tentativas), não de um valor aleatório novo a cada chamada. Antes de gerar e carregar os dados, a task apaga qualquer linha que já exista com esse `load_batch_id` — um no-op na primeira tentativa, e uma limpeza de verdade se a tentativa anterior tiver falhado no meio do `bcp` (rede caiu com metade dos 10M já carregados, por exemplo). Sem isso, um retry geraria um `load_batch_id` novo e nunca saberia que a tentativa anterior deixou linhas órfãs pra trás — o total da tabela ia inflando silenciosamente a cada falha.
- **A entrega pro ERP externo também não trava o worker.** Esgotadas as 5 tentativas do `tenacity` (ou um erro não-transiente do ERP), a mensagem vai pra `order-approved-dlq` em vez de derrubar o `order-approved-worker`.
- **Erros de processamento ficam registrados num log consultável, não só no stdout do container.** A tabela `pipeline_log` recebe uma linha toda vez que o `ingestion-consumer` desiste de uma mensagem (dado malformado, erro de banco não recuperável, falha ao publicar `ORDER_APPROVED`). Ver a seção "Log operacional" abaixo pra política de retenção.
- **Você fica sabendo quando a carga de 10M linhas termina, sem ficar olhando o Airflow.** A DAG `bulk_load_dimensions` manda uma mensagem no Telegram ao final — de sucesso ou de falha. Sem `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` configurados no `.env`, isso só é pulado (com um aviso no log), nunca quebra a DAG por causa disso.

## Log operacional

A tabela `pipeline_log` (`service`, `level`, `message`, `event_id`, `created_at`) guarda os erros que o `ingestion-consumer` não conseguiu resolver sozinho — mensagem malformada, erro de banco depois de esgotar as retentativas, falha ao publicar `ORDER_APPROVED`. É gravada numa conexão própria, separada da transação que falhou, e o próprio write é protegido: se o banco estiver genuinamente fora do ar, gravar o log também falharia, então isso cai de volta pro log do container em vez de mascarar o erro original.

Ela é propositalmente limitada — não é uma tabela de negócio, é diagnóstico, então não faz sentido deixá-la crescer sem controle. A DAG `pipeline_log_retention` (`airflow/dags/pipeline_log_retention.py`) roda de hora em hora e aplica duas regras:

1. Apaga tudo com mais de 7 dias.
2. Se ainda assim passar de 100.000 linhas, apaga as 200 mais antigas.

A rodada de cada hora é suficiente pra manter isso sob controle mesmo numa rajada de erros — 200 linhas por execução é uma margem confortável acima do que uma rajada real deveria gerar entre uma execução e outra.

## Monitoramento

Prometheus e Grafana sobem junto com o resto (fazem parte do `docker compose up -d --build`). Grafana fica em `http://localhost:3000` (login `admin`/`admin`, a menos que você tenha mudado `GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD` no `.env`) e já vem com a fonte de dados do Prometheus e um dashboard (`Pipeline de e-commerce`) provisionados — não precisa configurar nada na mão.

O que o dashboard mostra:

- **Infraestrutura**: CPU e memória por container (via `cAdvisor`), e se o exporter do SQL Server e o Redpanda estão respondendo ao scrape do Prometheus.
- **Pipeline de negócio**: taxa de webhooks recebidos por tipo de evento, taxa de eventos processados por resultado (sucesso, duplicata, erro de banco, mensagem malformada, falha de publicação — os mesmos rótulos que vão pro `pipeline_log`), taxa de entregas ao ERP externo por resultado, e taxa de pedidos recebidos pelo ERP fictício.

As métricas de negócio vêm direto dos 4 serviços Python (`prometheus_client`, endpoint `/metrics` em cada um — `webhook-receiver` e `mock-external-api` expõem no próprio host/porta HTTP; `ingestion-consumer` e `order-approved-worker` sobem um servidor de métricas à parte, nas portas `9100`/`9101`, só acessível dentro da rede do compose).

Duas ressalvas honestas:

- **Painéis de SQL Server/Redpanda são só de disponibilidade (`up`), não de performance.** Os nomes exatos das métricas que o `mssql-exporter` e o Redpanda expõem variam por versão, e eu não tenho como validar isso contra uma instância rodando de verdade aqui — em vez de arriscar um painel com uma métrica que não existe (e que renderiza vazio sem avisar por quê), deixei só a confirmação de que o Prometheus está conseguindo coletar de cada um. Dá pra abrir `http://localhost:9090/targets` pra confirmar os scrapes, olhar o `/metrics` de cada exporter e completar os painéis com os nomes reais.
- **Escalar o `ingestion-consumer` (`--scale ingestion-consumer=3`) faz o Prometheus enxergar só uma das réplicas.** O scrape aqui é estático (`ingestion-consumer:9100`), e a resolução de DNS do Compose não garante rodízio confiável entre múltiplas réplicas do mesmo serviço — pra métricas por réplica de verdade, precisaria de service discovery (Docker Swarm, ou um `file_sd` com IPs atualizados dinamicamente), fora do escopo deste projeto.

## Modelagem: o que cada tabela resolve

- **Chaves substitutas são `BIGINT IDENTITY`, não GUID.** O índice clusterizado do SQL Server é a ordem física das linhas na tabela; inserts com GUID aleatório fragmentam esse índice sob concorrência. Cada entidade externa mantém sua própria chave natural (`external_order_id`, `external_payment_id`, ...) como coluna `UNIQUE`, que serve também de chave de idempotência.
- **`address` é SCD Type 2**: toda alteração de endereço gera uma linha nova (`valid_from`/`valid_to`/`is_current`), nunca um `UPDATE` que apaga o histórico. Um índice único filtrado (`WHERE is_current = 1`) garante, no próprio banco, que só existe uma versão corrente por cliente. Um pedido referencia a versão específica do endereço vigente no momento em que foi criado, então uma edição futura nunca muda o retrato histórico de um pedido antigo.
- **Pedidos fechados são imutáveis.** Um gatilho (`trg_orders_block_retro_update`) rejeita qualquer `UPDATE` num pedido cujo status já seja terminal (`DELIVERED`, `CANCELLED`, `CLOSED`). A regra fica garantida no banco — não depende de nenhum serviço (nem futuro) lembrar de aplicá-la.
- `order_status_history` guarda a trilha de todas as transições de status, com o `event_id` do webhook que causou cada uma.

## A carga de 10 milhões de linhas

A tabela `order_history_fact` é analítica e separada da tabela transacional `orders` de propósito: carregar histórico sintético ali não interfere na demonstração de streaming, e as duas cargas de trabalho pedem estratégias de índice opostas (OLTP quer índice desde o início, carga em massa quer índice só depois).

A DAG (`airflow/dags/bulk_load_dimensions.py`) faz isso em quatro passos: cria a tabela se não existir, gera os dados e carrega, valida a contagem de linhas, e só então cria os índices secundários. O gerador de dados (`airflow/scripts/data_generator.py`) produz lotes de tamanho fixo (padrão 50 mil linhas) sem reter nada de lotes anteriores — o uso de memória fica achatado independente de quantas linhas você pedir no total.

A parte que realmente importa é como os dados chegam no banco. A primeira versão usava `pyodbc` com `cursor.fast_executemany = True`, que já é bem mais rápido que inserir linha a linha — mas ainda é um caminho de escrita totalmente logado: cada linha inserida é gravada por completo no log de transação. Rodando os 10 milhões de linhas: **9815 segundos (~2h43min, ~1019 linhas/s)**.

A versão atual troca isso por `bcp`, o utilitário de linha de comando do SQL Server, contra a tabela com a dica `-h TABLOCK`, banco em modo de recuperação `SIMPLE` (`db/entrypoint.sh`) e sem índices secundários no momento da carga — essas três condições juntas são o que faz o SQL Server tratar a carga como *minimamente logada* (o log registra só a extensão de disco alocada, não cada linha). Resultado: **3293 segundos (~55min, ~3038 linhas/s) — cerca de 3x mais rápido**, sendo ~522s pra gerar o arquivo intermediário e ~2770s no `bcp` em si.

A prova de que é minimal logging de verdade, e não só "ficou mais rápido por algum motivo": o tamanho do log de transação (`sys.dm_db_log_space_usage`) ficou exatamente igual antes e depois da carga dos 10 milhões de linhas. Uma carga totalmente logada desse volume não caberia num log de ~584MB sem pelo menos um crescimento automático.

Duas pegadinhas do `bcp` que valem registrar porque são fáceis de esbarrar:

1. `order_history_id` (auto-incremento) e `loaded_at` (valor padrão do próprio banco) não entram no arquivo de dados gerado. O `bcp` não aceita um arquivo com número de colunas diferente do da tabela a menos que você diga como mapear os campos — por isso existe `airflow/scripts/order_history_fact.fmt`, um format file que mapeia as 9 colunas geradas e deixa as outras duas por conta do banco.
2. Seria mais simples fazer o `bcp` carregar direto numa *view* com só as 9 colunas carregáveis, e pular o format file. Cheguei a cogitar isso e descartei: carga em massa através de view é sempre totalmente logada no SQL Server, não importa o `TABLOCK` nem o modelo de recuperação — anularia todo o ganho da mudança.

Vale registrar que ambas as medições foram feitas numa única máquina rodando a stack inteira junto (SQL Server, Redpanda, Postgres do Airflow, scheduler, webserver e os 4 serviços Python) — numa máquina dedicada só pro SQL Server, a diferença provavelmente seria ainda maior.

## O worker que fala com o ERP externo

`order-approved-worker` consome o tópico `order-approved` — publicado no instante em que o status de um pedido vira `APPROVED`, sem qualquer polling — e faz um `POST` com o payload completo (cliente, endereço, itens) pro ERP fictício. A resiliência a falhas temporárias usa `tenacity` com backoff exponencial e jitter, até 5 tentativas; se todas falharem, a mensagem vai pra um tópico de dead-letter em vez de travar a partição inteira ou ser descartada em silêncio. O `mock-external-api` tem uma taxa de falha configurável (`FAILURE_RATE`, padrão 30%) só pra dar ao retry algo de verdade pra provar.

## O que ainda falta (limitações honestas)

- **Outbox transacional de verdade não existe.** A publicação do evento `order-approved` acontece depois do commit da transação de negócio, mas fora dela. Existe uma janela pequena em que o pedido é aprovado no banco mas o processo cai antes de publicar no Kafka, e o ERP nunca fica sabendo. Uma versão de produção resolveria isso com uma tabela de outbox escrita na mesma transação e um relay/CDC separado publicando dali (ou Debezium fazendo CDC direto do log do SQL Server).
- **Eventos fora de ordem são descartados, não reprocessados.** Se `ORDER_STATUS_CHANGED` ou `PAYMENT_CONFIRMED` chegar antes do `ORDER_CREATED` correspondente, o evento é logado e ignorado. Uma versão real trataria isso com uma dead-letter topic com retry atrasado, não descarte silencioso.
- **Não tem testes automatizados além do gerador de dados** (`airflow/scripts/test_data_generator.py`). A cobertura de idempotência e concorrência hoje é validada rodando o `load-simulator` manualmente contra um ambiente de verdade, não por um teste que roda em CI.
- **A máquina de teste roda a stack inteira junto**, o que pode mascarar parte do ganho real da técnica de carga em massa — isso é uma limitação do ambiente, não motivo pra não medir.

## Rodando os testes e o lint

O único componente com testes automatizados hoje é o gerador de dados da carga em massa, porque é a única peça que não depende de nenhuma infraestrutura externa:

```bash
pip install pytest
cd airflow/scripts
pytest -v
```

Lint e checagem de tipos (config em `pyproject.toml`) rodam sobre o projeto inteiro:

```bash
pip install ruff mypy
ruff check .
mypy .
```
