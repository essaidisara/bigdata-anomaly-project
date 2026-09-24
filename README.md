# Détection d'Anomalies dans les Logs Web à l'Aide d'une Architecture Big Data Orientée Streaming

Projet académique — Module Big Data
École Nationale des Sciences Appliquées de Marrakech (ENSA-M), Université Cadi Ayyad
Filière Génie Cyberdéfense et Systèmes de Télécommunications Embarquées (GCDSTE)
Année universitaire 2025-2026 — Encadré par M. Aissam Bekkari

**Réalisé en équipe de 6 :** Bachir Soukaina, El Yamani Omayma, El Kissany Kaoutar, Lakbita Khadija, ElGadaoui Chaimaa, Essaidi Sara

---

##  Objectif

Concevoir et implémenter un pipeline Big Data hybride capable de traiter efficacement
des volumes massifs de logs web, tout en fournissant à la fois :
- une **surveillance en temps réel** pour la détection immédiate d'anomalies,
- une **analyse historique** (batch différé) des données persistées.

Le dataset utilisé est le **NASA web server access log** (août 1995), ~1,9 Go de
données au format Apache Common Log, représentant plusieurs millions de requêtes HTTP.

## Architecture


<img width="700" height="258" alt="image" src="https://github.com/user-attachments/assets/6181d7e0-e8f4-477e-9861-6ac4d643f7c7" />


Tous les composants sont déployés via **Docker Compose**.

##  Composants et rôles

| Composant | Rôle |
|---|---|
| Apache NiFi | Ingestion et découpage progressif des logs |
| Apache Kafka | Transport des données en streaming (topic `nasa_logs`) |
| Apache Spark (Structured Streaming) | Parsing, structuration et détection en temps réel |
| Elasticsearch | Indexation des données pour la recherche temps réel |
| Kibana | Visualisation et dashboard de supervision (type SOC) |
| HDFS | Stockage distribué et durable (format Parquet) |
| Hive | Data warehouse / requêtes SQL sur les données persistées |
| Docker / Docker Compose | Orchestration et reproductibilité de l'environnement |

##  Logique de détection d'anomalies

La détection combine deux approches afin de limiter les faux positifs :

**Règles unitaires (par requête), avec niveau de sévérité :**
- `HIGH` — erreur serveur (code HTTP ≥ 500)
- `MEDIUM` — transfert de données volumineux (> 50 Ko), accès à des fichiers
  sensibles (`.mpg`, `.zip`, `.exe`)
- `LOW` — erreur 404 isolée
- `INFO` — trafic normal

**Règles comportementales (par fenêtre temporelle) :**
- Plus de 10 erreurs 404 pour un même hôte sur une fenêtre de 5 minutes → `MEDIUM`
  (signature de scan applicatif)
- Plus de 50 requêtes pour un même hôte sur une fenêtre de 1 minute → `HIGH`
  (signature de déni de service ou d'activité automatisée)

**Principe directeur :** une anomalie isolée n'est pas systématiquement une attaque —
la criticité dépend du contexte et, pour certains cas, de la répétition dans le temps.

##  Dashboard Kibana

Un dashboard de supervision de type SOC a été construit, incluant :
- KPIs : nombre total de requêtes, nombre total d'anomalies, événements critiques (HIGH)
- Évolution temporelle du trafic et des anomalies
- Distribution des codes HTTP
- Top endpoints les plus sollicités
- Répartition des anomalies par sévérité
- Top hôtes générant le plus d'événements de sécurité

##  Analyse historique (Hive)

Deux tables externes exposent les données persistées dans HDFS :
- `processed_logs` — logs normaux (5 686 585 lignes)
- `anomalies` — événements détectés comme anormaux

Les requêtes HiveQL ont permis d'identifier que les anomalies sont majoritairement
de type `HIGH_BYTES` et `HTTP_404`, et de repérer les endpoints générant le plus
d'erreurs (ex. `/pub/winvn/readme.txt`, `/pub/winvn/release.txt`).

##  Limites et travaux futurs

- La détection repose sur des **règles à seuils statiques** (pas de machine learning).
- Aucun traitement **Spark Batch indépendant** n'a été implémenté : l'analyse
  différée s'appuie directement sur les données produites par le streaming.
- **Apache Superset**, prévu dans l'architecture cible pour le reporting BI,
  **n'a pas été finalisé** pour des raisons techniques liées à l'environnement
  de déploiement.
- Pistes d'amélioration identifiées : détection par machine learning, traitement
  Spark Batch dédié pour certaines analyses globales, intégration complète d'un
  outil de Business Intelligence.

##  Stack technique

Apache NiFi 1.25.0 · Apache Kafka (image confluentinc 7.6.0) · Apache Spark 3.5.1
(PySpark) · Elasticsearch 8.12.0 · Kibana 8.12.0 · Hadoop 3.2.1 (HDFS) · Apache Hive
(Metastore + HiveServer2, backend PostgreSQL) · Docker / Docker Compose



