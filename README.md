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
                     │ topic: order-approved                  │ 10M+ linhas, fast_executemany
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
contra o SQL Server, e a verificação do throughput de escrita (`fast_executemany`).
As instruções abaixo cobrem exatamente isso.

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
* **Escrita**: `pyodbc` com `cursor.fast_executemany = True` +
  `executemany` em lotes — o driver ODBC usa bind de parâmetros em array
  (bulk copy) em vez de um round-trip por linha, que é exatamente a técnica
  que o enunciado pede e que insert linha a linha não tem como entregar.
  Commit por lote (não por linha), o que limita o crescimento do log de
  transação sem pagar o overhead de commit por linha.
  * A tabela `order_history_fact` **não tem índices secundários na criação**
    — eles são criados **depois** da carga (`create_post_load_indexes`),
    porque manter índices atualizados a cada um dos 10M inserts é o custo
    evitável mais caro de uma carga em massa.
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
* Não medi throughput de escrita real do `fast_executemany` contra um SQL
  Server de verdade (só a geração dos dados) — depende de rodar
  `docker compose` numa máquina com Docker Hub acessível.
