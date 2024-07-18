1. Deploy the backend application:

```sh
sam build
sam deploy --guided --stack-name='ses-event-manager'
```

If you already have deployed stack

```sh
sam build 

```

```sh
sam deploy --config-env='default' # Para deployar a produccion
```

```sh
sam deploy --config-env='test' # Para deployar a pruebas
```


```sh
sam build
sam deploy --config-env='phoqo-produccion' --profile phoqo-produccion # Para deployar a phoqo-prod
```

```sh
sam sync --stack-name='ses-event-manager'
```



# Invocar localmente


```bash
sam local invoke ProcessSESQueue --event events/sqs_event.json -l log.log --profile phoqo-produccion
```

```bash
DOCKER_HOST=unix://$HOME/.docker/run/docker.sock sam local invoke  ProcessEmailSyncOS --event events/sync_dynamo_os.json -l log.log  --template template.yaml
```
