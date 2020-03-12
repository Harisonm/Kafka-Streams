# Applications à implémenter

Le Product Owner vous demande de lui fournir les informations suivantes

- Nombre de vues par film, catégorisées par type de vue:
    -  [x] arrêt en début de film (< 10% du film vu) 
        -  [x]  depuis le lancement
        -  [x] sur la dernière minute
        -  [x] sur les cinq dernières minutes 
    -  [x] arrêt en milieu de film (< 90% du film vu)
        -  [x] depuis le lancement
        -  [x] sur la dernière minute
        -  [x] sur les cinq dernières minutes
    -  [x] film terminé (> 90% du film vu) depuis le lancement
        -  [x] sur la dernière minute
        -  [x] sur les cinq dernières minutes
- Top 10
-  [ ] des films ayant les meilleurs retours (score > 4)
-  [ ] des films ayant les moins bons retours (score < 2)
-  [ ] Tip: vous aurez besoin d'une moyenne mobile pour calculer cela depuis le lancement du stream.

Tout ceci doit être exposé sur une API REST (format de sortie JSON) ayant le schéma suivant:

-  [ ] GET /movies/:id
    - Donne le nombre de vues et leur distribution pour un ID de film donné
```json
{
      "_id": 1,
      "title": "Movie title",
      "view_count": 200, 
      "stats": {
          "past": { 
              "start_only": 100, 
              "half": 10, 
              "full": 90
              },
          "last_minute": {
            "start_only": 80,
            "half": 2,
            "full": 0
            }, 
          "last_five_minutes": {
            "start_only": 2000,
            "half": 100, 
            "full": 90
        }
    }
}
```

-  [ ] GET /stats/ten/best/views
    - Donne les 10 meilleurs films de tous les temps selon leurs vues, trié par nombre de vues décroissant
```json
[
    {
    "score": 9.98 },
    {
    "title": "movie title 2", "score": 9.7
    }, {
    "score": 8.1 }
    ]
```
-  [ ] GET /stats/ten/best/views
    - Donne les 10 meilleurs films de tous les temps selon leurs vues, trié par nombre de vues décroissant
```json
    [
        {
        "title": "movie title 1",
        "views": 3500 },
        {
        "title": "movie title 2",
        "views": 2800
        }, 
        {
        "title": "movie title 3",
        "views": 2778 
        }
    ]
```

-  [ ] GET /stats/ten/worst/score
    - Donne les 10 pires films de tous les temps selon leur score moyen, trié par score croissant
```json
[
    {
    "title": "movie title 3",
    "score": 2.1 },
    {
    "title": "movie title 2",
    "score": 2.21
    },
    {
    "title": "movie title 3",
    "score": 3 
    }
]
```

-  [ ] GET /stats/ten/worst/views
    - Donne les 10 pires films de tous les temps selon leurs vues, trié par nombre de vues croissant
```json
[
    {
    "id": 2000,
    "title": "movie title 1", 
    "views": 2
    },
    {
    "id": 2,
    "title": "movie title 2",
    "views": 5
    }, 
    {
    "id": 12,
    "title": "movie title 3", 
    "views": 12
    }
]
```

## Scope
Il n'est pas demandé que l'application implémentée pour ces besoins gère le lancement d'instances multiples (ce qui impliquerait la gestion de stores distribués et l'usage de RPC).
Toutefois, un effort fait en ce sens pourra compter comme bonus.

## Documentation
- Kafka Stream DSL documentation: httpgis://docs.confluent.io/current/streams/developer-guide/dsl-api.html
- Kafka Stream Scala doc: https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#scala-dsl
- Kafka Stream Configuration Documentation:
https://docs.confluent.io/current/streams/developer-guide/config-streams.html
- Kafka Streams Interactive Queries: https://docs.confluent.io/current/streams/developer-guide/interactive-queries.html
- Akka HTTP (REST API): https://doc.akka.io/docs/akka-http/current/routing-dsl/index.html 
- Play JSON: https://www.playframework.com/documentation/2.8.x/ScalaJson