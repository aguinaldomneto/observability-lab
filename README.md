# Integração em Larga Escala — Protótipo E-commerce

Protótipo ponta a ponta (dados sintéticos) para o desafio técnico sênior:
ingestão de webhooks em alta concorrência, modelagem com auditoria histórica,
carga em massa de 10M+ linhas no SQL Server e integração event-driven de
saída, tudo com foco em performance.

## Arquitetura

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

Serviços: `webhook-receiver`, `redpanda` (+ `topics-init`), `ingestion-consumer`,
`order-approved-worker`, `mock-external-api`, `sqlserver`, `migrate` (Alembic,
one-shot), `airflow-postgres` + `airflow-webserver`/`airflow-scheduler`
(LocalExecutor), `load-simulator` (utilitário sob demanda).

## ⚠️ Limitação do ambiente onde este código foi escrito

Este código foi desenvolvido e validado **parcialmente** dentro de uma sessão
remota sandboxed (Claude Code on the web), que **bloqueia o pull de imagens
Docker genéricas** (Docker Hub, MCR) por política de rede do ambiente — o
daemon Docker até sobe, mas qualquer `docker pull`/`docker compose up` falha
com 403 no CDN de blobs do registry. Ou seja, **não foi possível rodar
`docker compose up` ponta a ponta dentro dessa sessão**, incluindo o SQL
Server real.

O que **foi de fato executado e validado** nesta sessão (sem depender de
containers de terceiros, usando apenas Python + PyPI, que são liberados):

* `alembic upgrade head --sql` — gera o DDL completo (tabelas, índice
  filtrado, trigger) e confirma que a migration compila e é sintaticamente
  válida para o dialeto `mssql`.
* Todos os módulos Python (`consumer.py`, `db_ops.py`, `worker.py`,
  `main.py` do webhook-receiver, `data_generator.py`) foram compilados
  (`py_compile`) e importados simulando o layout exato de dentro do
  container (paths, `sys.path`), pegando erros de import/sintaxe antes do
  build.
* O gerador de dados sintéticos dos 10M registros (Parte 3) foi *de fato*
  executado ponta a ponta, gerando os 10.000.000 de registros reais:
  **243.611 linhas/s, pico de 25.9 MiB de RSS** — memória plana, independente
  do total de linhas, porque nada além do batch atual (50k linhas) fica
  retido em memória. Isso prova o requisito de memória da Parte 3
  isoladamente da escrita no banco (que exige um SQL Server real para medir).
* `docker compose config` valida a sintaxe/estrutura do `docker-compose.yml`.

**O que você precisa rodar você mesmo** (em uma máquina com Docker e internet
normal, ou CI): `docker compose up --build`, a suíte de idempotência
(`load-simulator`), o trigger da DAG do Airflow com os 10M linhas reais
contra o SQL Server, e a verificação do throughput de escrita (`bcp` +
`TABLOCK`, ver seção "Versionamento e rollback" sobre a reescrita de
`fast_executemany` → `bcp`). As instruções abaixo cobrem exatamente isso.

## Como rodar

```bash
cp .env.example .env         # ajuste a senha do SA se quiser
docker compose up -d --build sqlserver redpanda topics-init
docker compose up --build migrate            # aplica as migrations (Alembic)
docker compose up -d --build webhook-receiver ingestion-consumer \
    order-approved-worker mock-external-api
docker compose up -d --build airflow-postgres airflow-init airflow-webserver airflow-scheduler
```

Airflow fica em `http://localhost:8080` (login `admin`/`admin` por padrão).
Dispare a DAG `bulk_load_dimensions` manualmente (ou via CLI:
`docker compose exec airflow-scheduler airflow dags trigger bulk_load_dimensions`).
Para um smoke test rápido antes de rodar os 10M completos:

```bash
docker compose exec airflow-scheduler airflow dags trigger bulk_load_dimensions \
    --conf '{"total_rows": 100000, "batch_size": 10000}'
```

### Testar ingestão, idempotência e concorrência

```bash
docker compose --profile tools run --rm load-simulator \
    python simulate.py --url http://webhook-receiver:8000/webhooks/events \
    --orders 2000 --concurrency 200 --duplicate-rate 0.15
```

Depois, confira no SQL Server:

```sql
SELECT COUNT(*) FROM orders;                    -- deve ser igual a --orders
SELECT COUNT(*) FROM processed_webhook_events;  -- menor que o total de requests
                                                 -- (os replays duplicados são no-op)
```

### Escalar o consumer

```bash
docker compose up -d --scale ingestion-consumer=3
```

O Redpanda rebalanceia as partições do tópico `webhook-events` entre as
réplicas automaticamente; cada partição pertence a um único consumer por vez,
então não há risco de duas réplicas processarem o mesmo pedido em paralelo.

## Versionamento e rollback

A reescrita da Parte 3 (`fast_executemany` → `bcp`/`TABLOCK`) é a primeira
mudança arquitetural depois que a carga de 10M linhas já tinha sido
validada de ponta a ponta contra o servidor de teste real — ou seja, o
ponto anterior era conhecido-bom e vale a pena poder voltar a ele sem
depender de memória do que mudou.

* **`e5c59ee`**: estado antes desta reescrita ("v1"). `pyodbc` +
  `cursor.fast_executemany = True`, medido em **9815,1s (~2h43min, 1019
  linhas/s)** para 10M linhas no servidor Debian 13 de teste — mais lento (é
  DML logado linha a linha), mas é a versão que já foi comprovadamente
  testada ponta a ponta, incluindo idempotência/concorrência (2000 pedidos
  simulados) e a Parte 4.
* **`0925e38`**: primeira versão da reescrita ("v2 inicial") — tinha dois
  bugs que só apareceram testando contra o SQL Server real: `bcp` sem
  `TrustServerCertificate` (SSL falhava) e depois packet size incompatível
  com TLS. Não é o commit pra usar como ponto de restauração da v2.
* **`48c3d48`** (`HEAD` deste branch): v2 com os dois bugs acima corrigidos
  (commits `9fa6fc3` e `48c3d48`) — **medida de ponta a ponta contra o SQL
  Server real: 3293,2s (~55min, 3038 linhas/s), ~3x mais rápido que a v1**,
  com log de transação comprovadamente sem crescer durante a carga (ver
  "Parte 3" acima). Este é o commit de referência da v2.

Criei localmente as tags anotadas `v1-fast-executemany` (`e5c59ee`) e
`v2-bcp-minimal-logging` (`48c3d48`) nesta sessão, mas **o `git push` das
tags foi rejeitado com 403** — a credencial desta sessão está autorizada só
para o branch designado, não para `refs/tags/*` — então elas não existem no
seu clone. Se quiser as tags de verdade (mais legível que decorar um SHA),
rode localmente depois de um `git fetch`:

```bash
git tag -a v1-fast-executemany -m "pré-bcp, fast_executemany, 9815.1s/10M" e5c59ee
git tag -a v2-bcp-minimal-logging -m "bcp + TABLOCK, minimal logging, 3293.2s/10M" 48c3d48
git push origin v1-fast-executemany v2-bcp-minimal-logging
```

**Para voltar para v1** se a v2 quebrar algo:

```bash
git checkout e5c59ee -- airflow/dags/bulk_load_dimensions.py \
    airflow/Dockerfile db/entrypoint.sh
git rm airflow/scripts/order_history_fact.fmt   # não existia em v1
# ou, para descartar totalmente os commits da v2 neste branch:
git reset --hard e5c59ee
```

Duas ressalvas sobre o que o rollback de código **não** desfaz sozinho:

1. `db/entrypoint.sh` roda `ALTER DATABASE ... SET RECOVERY SIMPLE` de forma
   idempotente (só altera se ainda não estiver em SIMPLE) — voltar o código
   para v1 não reverte esse ajuste no banco já em execução, porque v1 nunca
   gerenciava o recovery model. Se isso importar, rode manualmente
   `ALTER DATABASE [ecommerce] SET RECOVERY FULL;` ou recrie o volume
   `sqlserver_data` do zero.
2. O schema (`db/models.py`, migrations Alembic) **não muda** nesta
   reescrita — só o mecanismo de escrita da Parte 3. Então não há migration
   para reverter; qualquer linha já carregada em `order_history_fact` por
   uma versão continua legível pela outra.

## Parte 1 — Modelagem e migrations

* Migration versionada com **Alembic** (`db/migrations/`), schema definido em
  `db/models.py` (SQLAlchemy, fonte única de verdade).
* Chaves substitutas `BIGINT IDENTITY`, não GUID — em SQL Server o índice
  clusterizado é a ordem física das linhas; inserts com GUID aleatório
  fragmentam esse índice sob concorrência, exatamente o tipo de problema que
  este desafio testa. Cada entidade externa tem sua própria chave natural
  (`external_order_id`, `external_payment_id`, ...) como `UNIQUE`, que dobra
  como chave de idempotência.
* **`address` é SCD Type 2**: cada alteração de endereço gera uma nova linha
  (`valid_from`/`valid_to`/`is_current`), nunca um `UPDATE` destrutivo. Um
  índice único filtrado (`WHERE is_current = 1`) garante, no próprio banco,
  que só existe uma versão corrente por cliente/endereço lógico.
* **`orders` fechados são imutáveis**: um `AFTER UPDATE TRIGGER`
  (`trg_orders_block_retro_update`) rejeita qualquer `UPDATE` em um pedido
  cujo status atual já seja terminal (`DELIVERED`, `CANCELLED`, `CLOSED`).
  A regra fica garantida no banco, não depende de nenhum serviço lembrar de
  aplicá-la — inclusive este.
* Um pedido referencia a **versão específica do endereço** vigente no
  momento em que foi criado (`orders.address_id` aponta para uma linha
  versionada de `address`), então uma edição futura do endereço do cliente
  nunca altera o snapshot histórico do pedido.
* `order_status_history` guarda a trilha de auditoria de todas as
  transições de status, com o `event_id` do webhook que causou cada uma
  (rastreabilidade fim a fim).

## Parte 2 — Ingestão de streaming: por que Kafka (Redpanda) e não só FastAPI

O desafio pede para escolher e justificar. A escolha aqui foi
**Abordagem B (Kafka/Redpanda)**, com uma API HTTP fina na borda (necessária
porque o webhook em si é HTTP) que só publica no tópico — nunca toca o banco.

Motivos concretos, não só "Kafka é robusto":

1. **A escala declarada é "milhares de req/s"**. Uma API FastAPI que
   processa e grava no SQL Server dentro da própria requisição precisaria
   implementar, ela mesma, uma fila/buffer interno para não estourar o pool
   de conexões — ou seja, reconstruiria manualmente o que um message broker
   já resolve. Terminar a requisição do webhook em "publiquei no tópico"
   é O(1) e desacoplado da velocidade do banco.
2. **Backpressure de graça**: se o Redpanda ou a rede estiverem lentos,
   `producer.send_and_wait` simplesmente demora mais — isso naturalmente
   estrangula quem está chamando, sem fila não-controlada crescendo dentro
   do processo Python e sem qualquer conexão aberta ao SQL Server no
   caminho crítico do webhook.
3. **A Parte 4 pede um componente adicional, separado, orientado a
   eventos**. Com Kafka como espinha dorsal, o `ingestion-consumer` publica
   `order-approved` no instante em que o status muda — sem *polling* no
   banco. Com a Abordagem A, a Parte 4 exigiria inventar um segundo
   mecanismo de notificação (polling ou CDC) do zero.
4. **Escalar consumidores é nativo**: mais réplicas do `ingestion-consumer`
   no mesmo consumer group e o Redpanda rebalanceia partições sozinho, sem
   coordenação adicional.

Trade-off admitido: Kafka introduz mais uma peça de infraestrutura e uma
janela pequena "at-least-once" entre o consumer commitar a transação no
banco e comitar o offset (mitigada por processamento idempotente — replay
de um evento já processado é literalmente um no-op).

**Redpanda em vez de Kafka + Zookeeper**: mesmo protocolo, um único binário,
sem ZooKeeper, muito mais leve em memória/startup — a troca "mais leve, sem
fugir da ideia" que dá pra fazer aqui sem comprometer a arquitetura.

## Idempotência e concorrência (requisitos críticos)

* **Idempotência**: tabela `processed_webhook_events` (PK = `event_id` do
  parceiro) é escrita na **mesma transação** das gravações de negócio
  (`db_ops.claim_event`). Se o `event_id` já existe, a inserção viola a PK,
  a exceção vira `DuplicateEvent`, a transação é descartada e a mensagem é
  tratada como já processada — sem duplicar pedido/pagamento/fatura. Chaves
  naturais (`external_order_id`, `external_payment_id`,
  `external_invoice_id`) são uma segunda camada de proteção via `UNIQUE`
  constraint / `MERGE`.
* **Concorrência/backpressure**:
  * `common/db.py` usa `fast_executemany=True` e um **pool de conexões
    pequeno e limitado** (`pool_size=10, max_overflow=10` no consumer) — não
    importa quantas partições Kafka existam, o número de conexões
    simultâneas contra o SQL Server é sempre limitado por processo.
  * Transações curtas: cada evento é uma transação isolada, sem locks
    de longa duração esperando I/O externo.
  * `READ_COMMITTED_SNAPSHOT` é habilitado no banco pelo `migrate`
    (`db/entrypoint.sh`) — leituras concorrentes usam uma snapshot
    versionada em vez de bloquear atrás de escritores, reduzindo
    drasticamente o bloqueio leitor/escritor sob concorrência real (isso é
    o que realmente evita "travar o banco de dados de destino", mais do que
    só limitar conexões).
  * Offsets do Kafka só são commitados **depois** que a transação no SQL
    Server foi commitada — se o processo cair no meio de um lote, as
    mensagens não commitadas são reentregues, o que é seguro porque o
    processamento é idempotente (at-least-once + idempotência = efetivamente
    uma vez).

## Parte 3 — Carga de 10M linhas

* DAG `bulk_load_dimensions` (Airflow, `LocalExecutor` — ver justificativa
  no topo do arquivo da DAG: um Postgres leve como metadata DB, sem
  Celery/Redis, já que o broker "de verdade" da Parte 2/4 é o Redpanda).
* **Memória**: `airflow/scripts/data_generator.py` é um gerador Python puro
  (sem Faker — ver docstring do módulo para o porquê) que produz lotes de
  tamanho fixo (`batch_size`, padrão 50k); nenhuma linha de um lote anterior
  fica retida. **Testado de ponta a ponta nesta sessão**: 10.000.000 de
  linhas geradas, 243.611 linhas/s, pico de RSS de 25.9 MiB — plano,
  independente do total.
* **Escrita (v2 — `bcp` + `TABLOCK`, minimal logging)**: `generate_and_load`
  escreve os lotes gerados em um arquivo intermediário e chama `bcp` (utility
  client-side, `mssql-tools18`, instalado no `airflow/Dockerfile`) para
  carregar `dbo.order_history_fact` com o hint `-h "TABLOCK"`, banco em
  `RECOVERY SIMPLE` (`db/entrypoint.sh`) e **sem índices secundários** na
  tabela nesse momento — as três condições para o SQL Server qualificar a
  carga como *minimamente logada* (o log de transação registra só a extensão
  alocada, não cada linha, ao contrário do `fast_executemany`, que é rápido
  mas continua sendo DML totalmente logado linha a linha via TDS).
  `airflow/scripts/order_history_fact.fmt` é um format file do `bcp` que
  mapeia os 9 campos gerados para as colunas de destino e propositalmente
  **não** inclui `order_history_id` (IDENTITY) nem `loaded_at` (`DEFAULT
  SYSUTCDATETIME()`) — o `bcp` deixa essas duas para o servidor gerar
  sozinho. (Cogitei simplificar isso fazendo `bcp` contra uma *view* com só
  as 9 colunas carregáveis, em vez de um format file — descartei, porque
  carga em massa através de view é sempre totalmente logada no SQL Server,
  não importa o recovery model nem o `TABLOCK`; teria voltado à estaca
  zero.)
  * A tabela `order_history_fact` **não tem índices secundários na criação**
    — eles são criados **depois** da carga (`create_post_load_indexes`),
    porque manter índices atualizados a cada um dos 10M inserts é o custo
    evitável mais caro de uma carga em massa, e porque índices secundários
    presentes durante a carga também competem com o requisito de minimal
    logging acima.
  * **v1 desta task** usava `pyodbc` com `cursor.fast_executemany = True` +
    `executemany` em lotes (commit `e5c59ee`, ver "Versionamento e rollback"
    abaixo para voltar a ela). **Medida de ponta a ponta no servidor Debian
    13 do usuário**: os 10M linhas carregaram com sucesso em **9815,1s
    (~2h43min, 1019 linhas/s)**, tempo considerado alto demais para o
    critério de "performance máxima" do desafio — daí a reescrita para
    `bcp`.
  * **v2 (`bcp`) medida de ponta a ponta no mesmo servidor**: **3293,2s
    (~55min, 3038 linhas/s) — cerca de 3x mais rápido que a v1**, sendo
    521,8s para gerar o arquivo intermediário (~19.164 linhas/s) e 2770,0s
    no `bcp` propriamente dito (3610 linhas/s, média que o próprio `bcp`
    reporta). A prova de que é minimal logging de verdade, não só "ficou
    mais rápido por algum motivo": `total_log_size_in_bytes` em
    `sys.dm_db_log_space_usage` ficou **exatamente igual** (612.360.192
    bytes) antes e depois da carga dos 10M — o log de transação nunca
    precisou crescer. Uma carga totalmente logada de 10M linhas dessa
    largura não caberia num log de ~584MB sem pelo menos um auto-growth.
    Os dois números (v1 e v2) são de uma única execução cada, na mesma
    máquina de teste que roda todos os outros serviços do stack junto (ver
    "Limitações conhecidas" mais abaixo) — uma máquina dedicada
    provavelmente mostraria uma diferença ainda maior.
* **Validação**: a task `validate_row_count` conta as linhas do lote pelo
  `load_batch_id` e falha a DAG se vier abaixo do esperado.
* Parametrizável via `dag_run.conf`: `{"total_rows": ..., "batch_size": ...}`
  — útil para rodar um smoke test pequeno antes de disparar os 10M completos.

## Parte 4 — Integração event-driven de saída

`order-approved-worker` consome o tópico `order-approved` (publicado pelo
`ingestion-consumer` no instante em que o status vira `APPROVED` — orientado
a evento, não por polling) e faz `POST` do payload completo
(cliente + endereço + itens) para o ERP fictício (`mock-external-api`).
Resiliência a falha temporária via **tenacity** (backoff exponencial com
jitter, 5 tentativas); se as tentativas se esgotarem, a mensagem vai para
`order-approved-dlq` em vez de travar a partição ou ser descartada em
silêncio. `mock-external-api` tem uma taxa de falha configurável
(`FAILURE_RATE`, padrão 30%) propositalmente, para o retry ter algo real
para provar.

## Limitações conhecidas / próximos passos honestos

* A publicação do evento `order-approved` acontece **depois** do commit da
  transação de negócio, mas não dentro dela (não há uma tabela de outbox
  real + relay). Há uma janela pequena onde o commit no SQL Server acontece
  mas o processo cai antes de publicar no Kafka — o pedido fica `APPROVED`
  no banco sem nunca notificar o ERP. Uma versão de produção resolveria isso
  com um **Transactional Outbox** propriamente dito (tabela `outbox_events`
  escrita na mesma transação + um relay/CDC separado publicando dali), ou
  Debezium fazendo CDC direto do log do SQL Server. Não implementei isso
  aqui porque adicionaria uma peça de infraestrutura (Debezium + Kafka
  Connect) que eu não conseguiria validar neste ambiente de qualquer forma.
* Eventos fora de ordem (`ORDER_STATUS_CHANGED`/`PAYMENT_CONFIRMED` chegando
  antes do `ORDER_CREATED` correspondente) são logados e descartados
  (`db_ops` retorna `None`); o comentário no código deixa explícito que uma
  versão real trataria isso com uma dead-letter topic + retry com delay, não
  descarte silencioso.
* A reescrita da Parte 3 para `bcp`/`TABLOCK`/minimal logging (v2) **já foi
  medida de ponta a ponta** contra o SQL Server real do servidor de teste:
  3293,2s para os 10M linhas (~3x mais rápido que os 9815,1s da v1), com o
  log de transação sem crescer nem um byte durante a carga — ver "Parte 3"
  acima para os números completos. O caminho não foi limpo até chegar lá:
  o `bcp` primeiro falhou por TLS (certificado autoassinado, resolvido com
  `-u`), depois por packet size incompatível com TLS (resolvido caindo de
  65535 para 16384 bytes) — ambos os bugs e as correções estão no histórico
  de commits do branch.
* A máquina de teste roda todos os serviços juntos (SQL Server, Redpanda,
  Postgres do Airflow, scheduler, webserver, 4 serviços Python) — contenção
  de recursos pode mascarar o ganho real da técnica `bcp` em si. Isso é
  ambiental, não motivo para não medir.
