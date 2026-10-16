# Webhook Platform

API em Go para receber eventos e enviar webhooks em segundo plano. O PostgreSQL guarda os eventos e o histórico das entregas. O SQS organiza a fila, e o worker faz os envios HTTP.

O projeto ainda está em desenvolvimento.

## Como funciona

1. Um cliente envia um evento para a API, informando o tenant.
2. A API salva o evento e busca os endpoints inscritos naquele tipo de evento.
3. Cria uma entrega para cada endpoint e tenta colocar as entregas na fila.
4. O worker consome a fila, assina o payload com HMAC-SHA256 e faz o POST.
5. Respostas HTTP 2xx encerram a entrega com sucesso. As demais respostas e erros de conexão geram novas tentativas, até o limite de 5.

Um evento pode ter várias entregas. Cada entrega pode ter várias tentativas.

## Rodando localmente

Você precisa de Go compatível com a versão do `go.mod` (1.26.5) e Docker com Compose. Os comandos abaixo usam PowerShell e devem ser executados na raiz do projeto.

Suba os serviços:

```powershell
docker compose up -d
```

O ambiente local usa:

| Serviço | Endereço |
| --- | --- |
| API | http://127.0.0.1:8000 |
| PostgreSQL | 127.0.0.1:5433 |
| LocalStack / SQS | http://127.0.0.1:4566 |
| Redis | 127.0.0.1:6379 |

O Redis está no Compose, mas ainda não é usado pela aplicação.

Crie a fila caso ela ainda não exista:

```powershell
docker compose exec -T localstack awslocal sqs create-queue --queue-name deliveries --region us-east-1
```

Configure as variáveis no terminal em que vai iniciar a API:

```powershell
$env:AWS_REGION = "us-east-1"
$env:AWS_ENDPOINT_URL = "http://127.0.0.1:4566"
$env:QUEUE_URL = (docker compose exec -T localstack awslocal sqs get-queue-url --queue-name deliveries --region us-east-1 --query QueueUrl --output text).Trim()

go run ./cmd/api
```

Em outro terminal, configure as mesmas três variáveis e rode:

```powershell
go run ./cmd/worker
```

A aplicação não carrega o arquivo `.env` automaticamente. As variáveis precisam estar definidas no ambiente de cada processo. As credenciais AWS usadas pelo código são `test/test`, para o LocalStack.

As outras configurações são opcionais:

| Variável | Padrão |
| --- | --- |
| `ENV` | `development` |
| `HTTP_ADDR` | `:8000` |
| `DB_URL` | `postgres://postgres:postgres@127.0.0.1:5433/webhooks?sslmode=disable` |

## Criando um evento

Por enquanto, tenants e endpoints são cadastrados diretamente no banco. No pgAdmin, conecte ao banco `webhooks`, na porta `5433`, com usuário e senha `postgres`.

Exemplo de cadastro:

```sql
INSERT INTO tenants (id, name)
VALUES ('11111111-1111-1111-1111-111111111111', 'Cliente de teste')
ON CONFLICT (id) DO NOTHING;

INSERT INTO endpoints (tenant_id, url, secret, event_types)
VALUES (
    '11111111-1111-1111-1111-111111111111',
    'http://127.0.0.1:9000/webhook',
    'segredo-local',
    ARRAY['order.paid']
);
```

A URL precisa apontar para um servidor que aceite POST. Troque pelo endereço do seu receptor de teste. Executar o segundo INSERT novamente cria outro endpoint.

Com a API rodando:

```powershell
$body = @{
    type = "order.paid"
    payload = @{
        order_id = "123"
        amount = 150
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Method Post `
    -Uri "http://127.0.0.1:8000/v1/events" `
    -Headers @{ "X-Tenant-ID" = "11111111-1111-1111-1111-111111111111" } `
    -ContentType "application/json" `
    -Body $body
```

O retorno contém `event_id`, `deliveries` e `created`. Ele confirma a criação do evento, não o recebimento do webhook pelo destinatário. Com o worker desligado, a API continua aceitando eventos.

Para evitar eventos duplicados, envie também o header `Idempotency-Key`. Repetir a mesma chave no mesmo tenant retorna o evento existente com `created: false`.

Consulte as entregas no banco:

```sql
SELECT id, event_id, status, attempts, last_status_code, last_error, delivered_at
FROM deliveries
ORDER BY created_at DESC;

SELECT * FROM attempts ORDER BY created_at DESC;
```

## Testes

Testes automatizados e análise estática:

```powershell
go test ./...
go vet ./...
```

Para testar o fluxo com PostgreSQL e SQS locais:

```powershell
docker compose up -d postgres localstack
$env:WEBHOOK_INTEGRATION_TEST = "1"
go test ./internal/dispatcher -run '^TestWorkerIntegration$' -v -count=1
Remove-Item Env:WEBHOOK_INTEGRATION_TEST
```

Esse teste inicia seu próprio worker e receptor HTTP. Verifica entrega com sucesso, falha seguida de sucesso e falha após esgotar as tentativas. Usa 3 tentativas para terminar mais rápido, cria filas e registros exclusivos e remove esses dados no final. Não precisa deixar a API ou o worker rodando separadamente.

## Organização

- `cmd/api`: inicialização da API.
- `cmd/worker`: inicialização do consumidor da fila.
- `internal/httpapi`: rotas e handlers HTTP.
- `internal/domain`: modelos de evento, tenant, endpoint e entrega.
- `internal/db`: consultas SQL, conexão e esquema inicial.
- `internal/queue`: comunicação com SQS.
- `internal/dispatcher`: processamento das entregas e intervalo entre tentativas.
- `internal/signer`: assinatura HMAC do payload.
- `pkg/httpx`: cliente HTTP usado para enviar os webhooks.

## O que ainda falta

As rotas disponíveis são `GET /health` e `POST /v1/events`. Ainda faltam autenticação, cadastro de tenants e endpoints pela API e consulta de entregas por HTTP. O header `X-Tenant-ID` identifica o tenant, mas não autentica o cliente.

A API ainda ignora erros ao enfileirar. Uma entrega pode ficar salva no banco sem entrar no SQS; o contador `deliveries` não garante o enfileiramento. A gravação no banco e a publicação na fila precisam de um tratamento de consistência.

O SQL em `internal/db/migrations` roda automaticamente apenas quando o volume do PostgreSQL é inicializado vazio. Alterar esse arquivo e reiniciar o contêiner não reaplica a migration. Ainda não há um executor de migrations versionadas.

O Dockerfile e os workflows de CI e deploy ainda estão vazios.
