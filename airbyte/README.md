# Airbyte (abctl) – instalação manual

Com o Docker já instalado na máquina, use os passos abaixo para instalar o abctl e subir o Airbyte usando as credenciais do `.env`.

## 1. Instalar o abctl

No Linux:

```bash
curl -LsfS https://get.airbyte.com | bash -
```

Se o terminal pedir senha, digite. Ao terminar deve aparecer algo como `abctl install succeeded`.

Confirme:

```bash
abctl version
```

## 2. Gerar o secret a partir do .env

Na **raiz do projeto** (`/var/www/binder_data_hub`), carregue o `.env` e preencha o template `airbyte/secrets.yaml`:

```bash
set -a && . ./.env && set +a && envsubst '$AIRBYTE_ADMIN_PASSWORD,$AIRBYTE_CLIENT_ID,$AIRBYTE_CLIENT_SECRET' < airbyte/secrets.yaml > /tmp/airbyte-secrets.yaml
```

O arquivo com os valores reais fica em `/tmp/airbyte-secrets.yaml` (não vai para o repositório).

## 3. Instalar o Airbyte com esse secret

Defina a porta no `.env` (ex.: `AIRBYTE_PORT=8080`). Depois, com o `.env` já carregado no shell (ou na mesma sessão do passo 2):

```bash
abctl local install --secret /tmp/airbyte-secrets.yaml --port ${AIRBYTE_PORT:-8080}
```

A instalação pode levar alguns minutos. No fim, o Airbyte fica em **http://localhost:** + o valor de `AIRBYTE_PORT` (ex.: http://localhost:8080). Use o email que você informar no primeiro acesso e a senha definida em `AIRBYTE_ADMIN_PASSWORD` no `.env`.

## Resumo dos comandos (na raiz do projeto)

```bash
# Só na primeira vez: instalar abctl
curl -LsfS https://get.airbyte.com | bash -

# Carregar .env, gerar secret e instalar/atualizar Airbyte (porta vem de AIRBYTE_PORT no .env)
set -a && . ./.env && set +a && envsubst '$AIRBYTE_ADMIN_PASSWORD,$AIRBYTE_CLIENT_ID,$AIRBYTE_CLIENT_SECRET' < airbyte/secrets.yaml > /tmp/airbyte-secrets.yaml
abctl local install --secret /tmp/airbyte-secrets.yaml --port ${AIRBYTE_PORT:-8080}
```

## Comandos úteis

| Comando | Descrição |
|--------|-----------|
| `abctl local status` | Status do cluster e do Airbyte |
| `abctl local credentials` | Ver credenciais atuais (client_id, client_secret, etc.) |
| `abctl local uninstall` | Parar e remover (dados mantidos) |
