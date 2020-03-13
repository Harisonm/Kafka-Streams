# Running Kafka

Vous pouvez lancer un broker Kafka sur votre machine avec Docker.

D'abord, installez Docker (for Windows, Mac, ou le package linux), puis docker-compose (https://docs.docker.com/compose/install/).

Vous pouvez ensuite aller dans le dossier `docker`, puis faites :

```bash
docker-compose up -d
```

Vous pouvez arrêter le broker en faisant :

```bash
docker-compose stop
```

et le redémarrer en faisant:

```bash
docker-compose start -d
```