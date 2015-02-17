# Workshop NoSQL

*Découverte de [MongoDB](http://www.mongodb.org/) et d'[Elasticsearch](http://www.elasticsearch.org/), par la pratique !*


## Prise en main de MongoDB

Quelques rappels avant de démarrer :

* MongoDB est une base de donnnées NoSQL, orientée documents.
* Le format des documents est JSON.
* Les documents sont stockés dans des collections.
* Une base de données MongoDB peut contenir plusieurs collections de documents.
* Il n'est pas possible d'effectuer de jointures entre collections (et ce n'est pas la philosophie).

Dans tous les cas, n'hésitez pas à vous référer à la [documentation officielle](http://docs.mongodb.org/manual/reference/).

### Installation

Téléchargez la dernière version stable de MongoDB sur [mongodb.org/downloads](https://www.mongodb.org/downloads). Ce workshop est basé sur la version 2.6.6 de MongoDB.

Dézippez le bundle dans le dossier de votre choix, par exemple `$HOME/progz/mongodb-2.6.6`.

Les exécutables nécessaires au fonctionnement de MongoDB se trouvent dans le dossier `$HOME/progz/mongodb-2.6.6/bin`.

Pour plus de facilités, vous pouvez ajouter ce dossier à votre `PATH`, afin que les commandes `mongod` et `mongo` soient directement accessibles.
Par exemple sous Linux, ajoutez les lignes suivantes à votre fichier `.profile` :

```bash
# Path to MongoDB binaries
PATH="$HOME/progz/mongodb-2.6.6/bin:$PATH"
export PATH
```

Par défaut, MongoDB stocke ses données dans le dossier `/data/db`. Cela peut être modifié via le paramètre `--dbpath`

Vous pouvez donc créer un dossier spécifique pour stocker les données du workshop, par exemple `$HOME/data/nosql-workshop` :

```bash
mkdir -p "$HOME/data/nosql-workshop"
```

Démarrez MongoDB à l'aide de la commande suivante :

```bash
mongod --dbpath="$HOME/data/nosql-workshop"
```

### Prise en main du shell

MongoDB propose un shell Javascript interactif permettant de se connecter à une instance (démarrée via la commande `mongod`, comme précédemment).

Pour lancer le shell :

```bash
mongo
```

Par défaut, le shell se connecte à l'instance `localhost` sur le port `27017`, sur la base `test` :

```
MongoDB shell version: 2.6.6
connecting to: test
```

Le shell met à disposition un objet Javascript `db` qui permet d'interagir avec la base de données. Par exemple pour obtenir de l'aide :

```javascript
db.help()
```

Pour visualiser les bases disponibles :

```
show dbs
```

Pour changer de base de données, par exemple `workshop` (MongoDB crée automatiquement la base si elle n'existe pas) :

```
use workshop
```

Pour insérer un document dans une collection (la collection est créée automatiquement si elle n'existe pas encore) :

```javascript
db.personnes.insert({ "prenom" : "Jean", "nom" : "DUPONT" })
```

Pour afficher un document :

```javascript
db.personnes.findOne()
```

MongoDB génère automtiquement un identifiant unique pour chaque document, dans l'attribut `_id`. Cet identifiant peut être défini manuellement :

```javascript
db.personnes.insert({ "_id" : "jdupont", "prenom" : "Jean", "nom" : "DUPONT" })
```

Pour voir la liste des collections d'une base de données :

```
show collections
```

### Opérations CRUD

#### Recherche : find()

La méthode `find()` possède deux paramètres (optionnels) :

* le critère de recherche
* la projection (les attributs à retourner)

Par exemple, pour rechercher toutes les personnes se nommant "DUPONT" :

```javascript
db.personnes.find({ "nom" : "DUPONT" })
```

Si vous ne souhaitez retourner que les noms et prénoms, sans l'identifiant :

```javascript
db.personnes.find({ "nom" : "DUPONT" }, {"_id" : 0, "nom" : 1, "prenom" : 1})
```

Il est également possible de renommer un attribut dans la projection. Par exemple pour renomme le "nom" en "nom_de_famille" :

```javascript
db.personnes.find({ "nom" : "DUPONT" }, {"_id" : 0, "nom_de_famille" : "$nom", "prenom" : 1})
```

#### Insertion : insert()

L'insertion d'un document se fait via la méthode `insert()`, comme vu précédemment lors de la prise en main du Shell.

Le Shell étant un interpréteur Javascript, il est possible d'insérer plusieurs documents à l'aide d'une boucle `for` :

```javascript
for (var i = 1 ; i <= 100 ; i++) {
    db.personnes.insert({ "prenom" : "Prenom" + i, "nom" : "Nom" + i, "age" : (Math.floor(Math.random() * 50) + 20) })
}
```

#### Mise à jour

La mise à jour de documents se fait via la méthode `update()`, qui possède plusieurs paramètres :

* le filtre permettant de sélectionner les documents à mettre à jour
* la requête de mise à jour
* des options (par exemple : `{"multi" : true}` pour mettre à jour tous les documents correspondant au filtre)

Par exemple, pour répartir les personnes dans deux catégories ("Master" pour les plus de 40 ans, "Junior pour les autres") :

```javascript
db.personnes.update({"age" : { "$gte" : 40 }}, {"$set" : {"categorie" : "Master"}}, {"multi" : true})
db.personnes.update({"age" : { "$lt" : 40 }}, {"$set" : {"categorie" : "Junior"}}, {"multi" : true})
```

Remarque : MongoDB a créé automatiquement l'attribut "categorie" qui n'existait pas auparavant !

#### Suppression

La méthode `remove()` permet de supprimer des documents étant donné un filtre :

```javascript
db.personnes.remove({ "nom" : "DUPONT" })
```

Pour supprimer une collection :

```javascript
db.personnes.drop()
```

### Tableaux

Il est possible d'utiliser des tableaux dans les documents. Par exemple, on peut socker les compétences des personnes de la manière suivante :

```javascript
{
    "_id": "jdupont",
    "prenom": "Jean",
    "nom": "DUPONT",
    "competences" : [
        "Java",
        "Javascript",
        "HTML"
    ]
}
```

Pour rechercher les personnes possédant la compétence "Java" :

```javascript
db.personnes.find({ "competences" : "Java" })
```

Pour ajouter une compétence :

```javascript
db.personnes.update({ "_id" : "jdupont" }, {"$push" : {"competences" : "CSS"}})
```

Pour éviter les doublons :

```javascript
db.personnes.update({ "_id" : "jdupont" }, {"$addToSet" : {"competences" : "CSS"}})
```

Pour enlever une compétence :

```javascript
db.personnes.update({ "_id" : "jdupont" }, {"$pull" : {"competences" : "CSS"}})
```

## Prise en main d'Elasticsearch
Avant de démarrer : 

* ElasticSearch est un moteur de recherches distribué
* Il s'appuie sur une base de données NoSQL orientée documents
* Le format des documents est JSON
* ElasticSearch est basé sur la bibliothèque [Lucene](http://lucene.apache.org/)

Vous pouvez à tout moment vous référer à la [documentation officielle](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index.html).

### Installation
Téléchargez la dernière version d'ElasticSaerch sur [download.elasticsearch.org](https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.3.zip), ce workshop est basé sur la version 1.4.3.

Dézippez l'archive dans le dossier de votre choix, par exemple `$HOME/progz/elasticsearch-1.4.3`.

ElasticSearch étant basé sur le langage Java, veillez à disposer de **Java 8** installé sur votre machine. Vous pouvez vérifier l'installation de Java à l'aide de la commande `java -version`.

Les exécutables nécessaires au fonctionnement d'ElasticSearch se trouvent dans le dossier `$HOME/progz/elasticsearch-1.4.3/bin`. **elasticsearch** permet de lancer le server et **plugin** permet d'installer des plugins.

Avant de démarrer, installez les plugins suivants :

* head (administration)
```javascript
	plugin -i mobz/elasticsearch-head
```
* marvel (supervision & boite à outils)
```javascript
	plugin -i elasticsearch/marvel/latest
```

Le fichier `$HOME/progz/elasticsearch-1.4.3/config/elasticsearch.yml`, au format [YAML](http://fr.wikipedia.org/wiki/YAML) permet de configurer ElasticSearch, activez (décommentez) l'option suivante. Pour le reste, la configuration par défaut nous suffit pour l'instant.
```javascript
	discovery.zen.ping.multicast.enabled: false
```

Vous pouvez à présent démarrer le serveur.
```javascript
	elasticsearch
```

Il est possible d'ajouter des options Java pour augmenter la mémoire allouée à ElasticSearch en passant directement les paramètres de la JVM à l'exécutable ElsasticSearch.
```javascript
	elasticsearch -Xmx=2G -Xms=2G
```

Pour vérifier le démarrage de votre noeud ElasticSearch, 
```javascript
	http://localhost:9200/
```

Vous devriez obtenir une réponse qui ressemble à celle là : 
```javascript
	{
	  "status" : 200,
	  "name" : "Spinnerette",
	  "cluster_name" : "elasticsearch",
	  "version" : {
	    "number" : "1.4.3",
	    "build_hash" : "36a29a7144cfde87a960ba039091d40856fcb9af",
	    "build_timestamp" : "2015-02-11T14:23:15Z",
	    "build_snapshot" : false,
	    "lucene_version" : "4.10.3"
	  },
	  "tagline" : "You Know, for Search"
	}
```

Pour accéder aux plugins précédemment installés il vous suffit de consulter les URLs suivantes : 

* [http://localhost:9200/_plugin/head/](http://localhost:9200/_plugin/head/)
* [http://localhost:9200/_plugin/marvel/](http://localhost:9200/_plugin/marvel/)

### Concepts essentiels

#### Document
Un document est un élément unitaire, au format JSON, stocké dans ElasticSearch.

#### Noeud
Un noeud est une instance d'ElasticSearch, une noeud appartien à un **cluster**.

#### Cluster
Un cluster est composé d'un ou plusieurs **noeuds** ElasticSearch qui sont connectés entre eux et qui partagent le même nom. Un cluster comporte un noeud maître unique (**master node**). En cas de défaillance du master node, un nouveau master node est élu parmis les noeuds restants.

#### Index
Un index est un regroupement logique d'un ensemble de documents. Un index est composé de **shards**. Tous les documents appartienent à un index.

#### Type
Un type est un sous-ensemble d'un index qui permet de regrouper des documents. De la même manière que pour les index, les types permettent de configurer le stockage des documents. Tout document appartient à un type.

#### Shard
Un shard est un fragment d'un index. Ce sont les shards qui permettent de partitionner les index sur plusieurs noeuds. Ainsi, un index peut être partitionner sur autant de noeuds que cet index comporte de shards. Le nombre de shards par défaut est de **5**.

#### Réplique
Une réplique est une copie intégrale d'un index. Les répliques permettent d'augmenter la tolérance à la panne du système ainsi que la durabilité des données. Une réplique comporte autant de shards que l'index original. Le nombre de répliques par défaut est de **1**.

### Prise en main de l'API

ElasticSearch expose l'ensemble de ses APIs à l'aide d'un API [REST](http://www.pompage.net/traduction/comment-j-ai-explique-rest-a-ma-femme), il est donc possible d'utiliser n'importe quel client HTTP pour manipuler ElasticSearch.

Le plugin **Marvel** que nous avons installé plus tôt propose parmis ces outils, le client **Sense** qui offre des fonctionnalités qui facilitent l'utilisation d'ElasticSearch. 

	http://localhost:9200/_plugin/marvel/sense/index.html

Votre client préféré, si vous en avez un, fera sans problèmes l'affaire :D.

#### Les convetions
L'API d'ElasticSearch est composé d'un ensemble d'APIs qui exposent des opérations spécialisées. Cette API est conforme aux standards REST. Elle est orientée ressources, s'appuie sur les verbes HTTP, les codes de retours HTTP...

D'une manière générale, les requêtes ressemblent à ça : 

	http://[host]:[port]/index/type/_action|id

Par exemple :

	localhost:9200/heroes/person/_search
	localhost:9200/heroes/person/_count
	localhost:9200/heroes/person/ironman


Les actions sur les index permettent généralement d'effectuer l'opération sur plusieurs index simultanément. Par exemple, pour effectuer une requête sur les index index1 et index2, il est possible d'utiliser l'URL suivante :

	localhost:9200/heroes,vilains/_search

Vous trouverez de nombreux exemples (inclusions, exclusions, jokers, ...) dans la documentation.

### CRUD
#### Insertion
Pour insérer un document, on utilise la requête suivante.

```javascript
	curl -XPOST 'localhost:9200/heroes/person/ironman' -d '{
		"firstName" : "Tony",
		"lastName" : "Stark"
	}'
```

Le verbe, **POST**, indique qu'on insert un document
L'URL est construite de la manière suivante : 

	<host>:<port>/<index>/<type>/<id>

Si l'index n'existe pas au moment de la création du document, celui-ci est créé automatiquement.

#### PUT vs POST
Pour l'insertion de données, les verbes **POST** et **PUT** sont équivalents. Le verbe **POST** permet d'insérer des documents sans spéficier l'identifiant du document.

```javascript
	curl -XPOST 'localhost:9200/heroes/person/' -d '{
		"firstName" : "Charles",
		"lastName" : "Xavier"
	}'
```

La réponse renvoyée contient l'identifiant généré par ElasticSearch.

```javascript
	{
	   "_index": "heroes",
	   "_type": "person",
	   "_id": "AUuFm0z0oSZRHss7_tP7",
	   "_version": 1,
	   "created": true
	}
```

#### Extraction
Pour extraire un document à l'aide de son identifiant, on utilise la requête suivante. 

```javascript
	curl -XGET 'localhost:9200/heroes/person/ironman'
```

La réponse renvoyée est la suivante : 

```javascript
	{
	   "_index": "heroes",
	   "_type": "person",
	   "_id": "ironman",
	   "_version": 11,
	   "found": true,
	   "_source": {
	      "firstName": "Tony",
	      "lastName": "Stark"
	   }
	}
```

L'attribut `found` indique que le document a bien été trouvé (`true` dans notre cas, `false` si le document n'a pas été trouvé). L'attribut `_source` contient le document extrait. 

#### Mise à jour
Pour mettre à jour les données, il est possible d'utiliser les requêtes **PUT** et **POST** présentées ci-dessus. Cette méthode permet de mettre à jour l'ensemble du document.

Il est possible d'effectuer des mises à jour partielles en utilisant l'API `_update`.

```javascript
	curl -XPOST 'localhost:9200/heroes/person/ironman/_update' -d '{
		"doc" : {
			"firstName" : "Tomy"
		}
	}'
```

#### Suppression
Pour supprimer un document, on utilise le verbe **DELETE**

	curl -XDELETE 'localhost:9200/heroes/person/ironman'

### Exists
Il est possible, à l'aide du verbe **HEAD** de vérifier l'existance d'un document.

	curl -XHEAD 'localhost:9200/heroes/person/ironman'

Les statut renvoyé : 

* **200** indique que le document existe
* **404** indique que le document n'existe pas


## API de Recherche
Commençons par insérer quelques données ...
	
```javascript
	curl -XPOST 'http://localhost:9200/heroes/person/ironman' -d '{"firstName":"Tony","lastName":"Stark","aka":"Iron Man","team":"Avengers","age":45}'
	curl -XPOST 'http://localhost:9200/heroes/person/thor' -d '{"firstName":"Thor","lastName":"Odinson","aka":"Thor","team":"Avengers","age":27}'
	curl -XPOST 'http://localhost:9200/heroes/person/antman' -d '{"firstName":"Hank","lastName":"Pym","aka":"Ant-Man","team":"Avengers","age":41}'
	curl -XPOST 'http://localhost:9200/heroes/person/wasp' -d '{"firstName":"Janet","lastName":"van Dyne","aka":"Wasp","team":"Avengers","age":32}'
	curl -XPOST 'http://localhost:9200/heroes/person/hulk' -d '{"firstName":"Bruce","lastName":"Banner","aka":"Hulk","team":"Avengers","age":41}'
	curl -XPOST 'http://localhost:9200/heroes/person/misterfantastic' -d '{"firstName":"Reed","lastName":"Richards","aka":"Mister Fantastic","team":"FantasticFour","age":45}'
	curl -XPOST 'http://localhost:9200/heroes/person/invisiblewoman' -d '{"firstName":"Susan","lastName":"Storm","aka":"Invisible Woman","team":"FantasticFour","age":29}'
	curl -XPOST 'http://localhost:9200/heroes/person/thehumantorch' -d '{"firstName":"Johnny","lastName":"Storm","aka":"The Human Torch","team":"FantasticFour","age":27}'
	curl -XPOST 'http://localhost:9200/heroes/person/thething' -d '{"firstName":"Ben","lastName":"Grimm","aka":"The Thing","team":"FantasticFour","age":42}'
```

L'API `_search` permet d'effectuer des recherches dans ElasticSearch.

### Recherche
La requête suivante lance une recherche sur l'ensemble des documents de type `person` dans l'index `heroes` (par défault, une recherche remonte 10 résultats) : 

	curl -XPOST 'http://localhost:9200/heroes/person/_search'

La requête suivante permet de rechercher tous ls documents qui ont un attribut `lastName` dont la valeur est `storm` : 

```javascript
	curl -XPOST 'http://localhost:9200/heroes/person/_search' -d '{
	    "query": {
	        "match": {
	           "lastName": "storm"
	        }
	    }
	}'
```

La requête suivante permet d'effectuer une recherche sur les documents dont le prénom commence par un `t` : 

```javascript
	curl -XPOST 'http://localhost:9200/heroes/person/_search' -d '{
	    "query": {
	        "wildcard": {
	           "firstName": {
	              "value": "t*"
	           }
	        }
	    }
	}
```

Il est possible de faire des recherches à plusieurs niveaux :
 
* sur un type et un index donnés : `localhost:9200/heroes/person/_search`
* sur l'ensemble des types d'un index donné : `localhost:9200/heroes/_search`
* sur l'ensemble des index d'un cluster : `localhost:9200/_search`

### Agrégations
Les agrégations permettent d'agglomérer des données et d'effectuer des calculs à la volée sur documents contenus dans les index.

Bien qu'il soit possible de combiner recherche et aggrégations, nous ne nous intéressons pas ici aux recherches (d'où l'attribut `"size": 0` ...).

Obtenir la répartition des valeurs du terme `team` dans les documents de type `person` de l'index `heroes` : 

```javascript
	curl -XPOST 'http://localhost:9200/heroes/person/_search' -d '{
	    "size": 0,
	    "aggs" : {
	        "teams" : {
	            "terms": {
	                "field": "team"
	            }
	        }
	    }
	}
```

Il est possible de faire des sous-agrégations. Obtenir la répartition des valeurs du terme `lastName` dans la répartion du terme `team` dans les documents de type `person` de l'index `heroes` : 

```javascript
	curl -XPOST 'http://localhost:9200/heroes/person/_search' -d '{
	    "size": 0,
	    "aggs" : {
	        "teams" : {
	            "terms": {
	                "field": "team"
	            },
				"aggs" : {
	                "names" : {
	                    "terms": {
	    	                "field": "lastName"
	    	            }
	                }
				}
	        }
	    }
	}
```

Il est possible de faire des calculs avec les aggrégations. L'âge moyen des membres de chaque équipe : 

```javascript
	curl -XPOST 'http://localhost:9200/heroes/person/_search' -d '{
	    "size": 0,
	     "aggs" : {
	         "teams" : {
	            "terms": {
	                "field": "team",
	                "order" : { "avgAge" : "desc" }
	            },
				"aggs" : {
	                "avgAge" : { 
	                    "avg" : { 
	                        "field" : "age"   
	                    }
	                }
				}
	        }
	    }
	}
```

## Application Java

L'objectif est de développer une application manipulants des données relatives aux installations sportives de la région Pays de la Loire.

Les données sont issues de [http://data.paysdelaloire.fr](http://data.paysdelaloire.fr).

Trois jeux de données vont particulièrement nous intéresser et sont disponibles dans le projet (format CSV) :

* [Installations](http://data.paysdelaloire.fr/donnees/detail/equipements-sportifs-espaces-et-sites-de-pratiques-en-pays-de-la-loire-fiches-installations)
* [Equipements](http://data.paysdelaloire.fr/donnees/detail/equipements-sportifs-espaces-et-sites-de-pratiques-en-pays-de-la-loire-fiches-equipements)
* [Activités](http://data.paysdelaloire.fr/donnees/detail/equipements-sportifs-espaces-et-sites-de-pratiques-en-pays-de-la-loire-activites-des-fiches-equ)

Des liens existent entre les trois jeux de données :

* une installation possède un ou plusieurs équipements
* une ou plusieurs activités peuvent être pratiquées sur un équipement donné.

![model](assets/model.png)

### Import des données dans MongoDB

La première tâche consiste à créer la collection des installations sportives à partir des trois fichiers CSV, en utilisant le driver MongoDB natif Java.

Pour cela, recherchez les `TODO` dans le code du module `batch`, complétez le code pour obtenir des documents de cette forme :

```javascript
{
    "_id": "440390003",
    "nom": "La Pierre Tremblante",
    "adresse": {
        "numero": "",
        "voie": "Chemin des rives",
        "lieuDit": "",
        "codePostal": "44640",
        "commune": "Cheix-en-Retz"
    },
    "location": {
        "type": "Point",
        "coordinates": [
            -1.816274,
            47.181243
        ]
    },
    "multiCommune": false,
    "nbPlacesParking": 0,
    "nbPlacesParkingHandicapes": 0,
    "dateMiseAJourFiche": ISODate("2014-06-18T00:00:00Z"),
    "equipements": [
        {
            "numero": "191989",
            "nom": "La Pierre tremblante",
            "type": "Point d'embarquement et de débarquement isolé",
            "famille": "Site d'activités aquatiques et nautiques",
            "activites": [
                "Canoë de randonnée",
                "Pêche au coup en eau douce"
            ]
        }
    ]
}
```

### Services Java

La seconde tâche consiste à implémenter les services Java utilisés par les pages web de l'application.

Pour cela, recherchez les `TODO` dans le code du module `application` et complétez le code manquant.

L'application web propose une page "API Checkup" permettant de vérifier que les services répondent correctement.


### Recherche full-text

#### Avec MongoDB

On peut par exemple positionner un index de type "text" sur le nom de l'installation et sa commune, en mettant un poids plus important pour la commune :

```javascript
db.installations.ensureIndex(
    {
        "nom" : "text",
        "adresse.commune" : "text"
    },
    {
        "weights" : {
            "nom" : 3,
            "adresse.commune" : 10
        },
        "default_language" : "french"
    }
)
```

Ensuite, on peut par exemple rechercher les "Ports" de la ville de "Carquefou", en triant par pertinance et en ne conservant que les 10 premiers résultats :

```javascript
db.installations.find(
    {
        "$text": {
            "$search": "Port Carquefou",
            "$language" : "french"
        }
    },
    {
        "score": {"$meta": "textScore"}
    }
)
.sort({"score": {"$meta": "textScore"}})
.limit(10)
```

#### Avec Elasticsearch
##### Création du mapping
On crée d'abord le mapping. Dans cette première version, nous nous contenterons d'un mapping simple : 

```javascript
	curl -XPOST 'http://localhost:9200/installations' -d '{
		"mappings": {
			"installation": {
				"properties": {
					"location": {
						"properties": {
							"coordinates": {
								"type": "geo_point"
							}
						}
					}
				}
			}
		}
	}'
```

#### Import des données dans Elasticsearch

Le job **MongoDbToElasticsearch** a pour objectif de gérer la copie des données de MongoDB à ElasticSearch. Nous ne cherchons pas ici à gérer une mise à jour incrémentale des données. 

Nous souhaitons extraire l'ensemble des données de la collection `installations` et les écrire dans l'index `installations` (type `installation`). Afin d'éviter pour le moment des problèmes de conversion de dates, nous filtrerons la propriété `dateMiseAJourFiche` avant l'insersion dans **ElasticSearch**.

Une fois les documents indexés dans ElasticSearch, nous pouvons lancer recherche full text : 

```javascript
	curl -XPOST 'http://localhost:9200/installations/installation/_search' -d '{
	    "query": {
	        "multi_match": {
	           "query": "Carquefou",
	           "fields": ["_all"]
	        }
	    }
	}'
```

### Recherche géographique

#### Avec MongoDB

Tout d'abord, il faut positionner un index géographique de type `2dsphere` sur l'attribut "location" de la collection des installations :

```javascript
db.installations.ensureIndex( { "location" : "2dsphere" } )
```

Ensuite, si l'on souhaite rechercher les installations sportives autour de Carquefou (lat = 47.3, lon = -1.5), dans un rayon de 5km :

```javascript
db.installations.find({ "location" : 
    { $near :
        { $geometry :
            { type : "Point" ,
              coordinates : [ -1.5 , 47.3 ]
            },
            $maxDistance : 5000
        }
    }
})
```

#### Avec Elasticsearch

Pour effectuer la même requête dans ElasticSearch : 

```javascript
	curl -XPOST 'http://localhost:9200/installations/installation/_search' -d '{
		"query" : {
			"filtered" : {
		        "query" : {
		            "match_all" : {}
		        },
		        "filter" : {
		            "geo_distance" : {
		                "distance" : "5km",
		                "coordinates" : [ -1.5 , 47.3 ]
		            }
			    }
			}
	    }
	}'
```