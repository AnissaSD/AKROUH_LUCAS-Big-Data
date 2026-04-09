# Évaluation n°3 R6.01 — Big Data : enjeux, stockage et extraction

**Groupe :** LUCAS Azad & AKROUH Anissa

**Sujet :** Analyse du fichier `tags.csv` avec Hadoop MapReduce pour répondre à cinq questions sur les tags de films.

---

## 1. Commandes utilisées

### Question 1 — Combien de tags chaque film possède-t-il ?

```python
from mrjob.job import MRJob

class Q1TagsPerMovie(MRJob):

    def mapper(self, _, line):
        try:
            if line.startswith("userId,movieId,tag,timestamp"):
                return
            parts = line.strip().split(",", 3)
            if len(parts) != 4:
                return
            userId, movieId, tag, timestamp = parts
            yield movieId, 1
        except Exception:
            pass

    def reducer(self, movieId, counts):
        yield movieId, sum(counts)

if __name__ == "__main__":
    Q1TagsPerMovie.run()
```

Commandes Hadoop :

```bash
# Créer un échantillon pour test
head -20 ml-25m/tags.csv > tags_sample.csv

# Test local sur l'échantillon
python q1_tags_per_movie.py tags_sample.csv

# Uploader le fichier dans HDFS
hdfs dfs -mkdir -p /datasets/exam
hdfs dfs -put -f ml-25m/tags.csv /datasets/exam/


# Lancer le job Hadoop (configuration par défaut)
python q1_tags_per_movie.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///datasets/exam/tags.csv \
  -o hdfs:///datasets/exam_out_q1

# Consulter un extrait du résultat
hdfs dfs -cat /datasets/exam_out_q1/part-00000 | head

# Extraire tout le résultat
hdfs dfs -getmerge /datasets/exam_out_q1 q1_tags_per_movie_full.txt
```

---

### Question 2 — Combien de tags chaque utilisateur a-t-il ajoutés ?

```python
from mrjob.job import MRJob

class Q2TagsPerUser(MRJob):

    def mapper(self, _, line):
        try:
            if line.startswith("userId,movieId,tag,timestamp"):
                return
            parts = line.strip().split(",", 3)
            if len(parts) != 4:
                return
            userId, movieId, tag, timestamp = parts
            yield userId, 1
        except Exception:
            pass

    def reducer(self, userId, counts):
        yield userId, sum(counts)

if __name__ == "__main__":
    Q2TagsPerUser.run()
```

Commandes Hadoop :

```bash
# Test local
python q2_tags_per_user.py tags_sample.csv


# Lancer le job Hadoop (configuration par défaut)
python q2_tags_per_user.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///datasets/exam/tags.csv \
  -o hdfs:///datasets/exam_out_q2

# Extraire le résultat
hdfs dfs -getmerge /datasets/exam_out_q2 q2_tags_per_user_full.txt
```

---

### Question 3 — Combien de blocs le fichier occupe-t-il dans HDFS ?

```bash
# Vérifier la taille du fichier dans HDFS
hdfs dfs -du /datasets/exam/tags.csv

# Vérifier la taille de bloc par défaut
hdfs getconf -confKey dfs.blocksize

# Obtenir les métadonnées du fichier (taille réelle + taille de bloc)
hdfs dfs -stat "%b %o %n" /datasets/exam/tags.csv

# Calculer le nombre de blocs avec la configuration par défaut (128 Mo)
python -c "import math; print(math.ceil(38810332 / 134217728.0))"

# Calculer le nombre de blocs avec une taille de bloc de 64 Mo
python -c "import math; print(math.ceil(38810332 / 67108864.0))"

# Uploader le fichier avec une taille de bloc de 64 Mo
hdfs dfs -D dfs.blocksize=67108864 -put -f ml-25m/tags.csv /datasets/exam/tags_64mo.csv
```

---

### Question 4 — Combien de fois chaque tag a-t-il été utilisé pour taguer un film ?

```python
from mrjob.job import MRJob

class Q4TagCount(MRJob):

    def mapper(self, _, line):
        try:
            if line.startswith("userId,movieId,tag,timestamp"):
                return
            parts = line.strip().split(",", 3)
            if len(parts) != 4:
                return
            userId, movieId, tag, timestamp = parts
            yield tag, 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)

if __name__ == "__main__":
    Q4TagCount.run()
```

Commandes Hadoop — configuration par défaut :

```bash

# Lancer le job Hadoop
python q4_tag_count.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///datasets/exam/tags.csv \
  -o hdfs:///datasets/exam_out_q4

# Extraire le résultat
hdfs dfs -getmerge /datasets/exam_out_q4 q4_tag_count_full.txt
```

Commandes Hadoop — bloc 64 Mo :

```bash

# Lancer le job sur le fichier uploadé avec bloc de 64 Mo
python q4_tag_count.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///datasets/exam/tags_64mo.csv \
  -o hdfs:///datasets/exam_out_q4_64

# Extraire le résultat
hdfs dfs -getmerge /datasets/exam_out_q4_64 q4_tag_count_full_64.txt

```

---

### Question 5 — Pour chaque film, combien de tags le même utilisateur a-t-il introduits ?

```python
from mrjob.job import MRJob

class Q5UserTagsPerMovie(MRJob):

    def mapper(self, _, line):
        try:
            if line.startswith("userId,movieId,tag,timestamp"):
                return
            parts = line.strip().split(",", 3)
            if len(parts) != 4:
                return
            userId, movieId, tag, timestamp = parts
            yield movieId + "," + userId, 1
        except Exception:
            pass

    def reducer(self, movie_user, counts):
        yield movie_user, sum(counts)

if __name__ == "__main__":
    Q5UserTagsPerMovie.run()
```

Commandes Hadoop — configuration par défaut :

```bash

# Lancer le job Hadoop
python q5_user_tags_per_movie.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///datasets/exam/tags.csv \
  -o hdfs:///datasets/exam_out_q5_default

# Extraire et renommer
hdfs dfs -getmerge /datasets/exam_out_q5_default result_user_tags_per_movie_default.txt
```

Commandes Hadoop —  64 Mo :

```bash


# Lancer le job Hadoop
python q5_user_tags_per_movie.py -r hadoop \
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
  hdfs:///datasets/exam/tags_64mo.csv \
  -o hdfs:///datasets/exam_out_q5

# Extraire et renommer
hdfs dfs -getmerge /datasets/exam_out_q5 result_user_tags_per_movie.txt
```

---

## 2. Commentaires et explications sur la démarche suivie

### Préparation et téléchargement du fichier

Avant tout, nous avons vérifié que le fichier `tags.csv` du dataset MovieLens ml-25m était bien disponible localement sur la VM (`~/ml-25m/tags.csv`). Le fichier contient les colonnes `userId`, `movieId`, `tag` et `timestamp`.

Pour éviter les longues exécutions lors de la mise au point des scripts, nous avons créé un fichier d'échantillon réduit de 20 lignes :

```bash
head -20 ml-25m/tags.csv > tags_sample.csv
```

Ce fichier nous a permis de tester chaque script localement avant de l'exécuter sur le cluster Hadoop avec le fichier complet.

### Question 1 — Tags par film

Nous avons écrit un script MapReduce avec `mrjob`. Le mapper lit chaque ligne, ignore l'en-tête, extrait le `movieId` et émet la paire `(movieId, 1)`. Le reducer somme toutes les valeurs associées à chaque `movieId`, ce qui donne directement le nombre de tags pour chaque film.

Un point important : lors d'une première tentative, le script ne fonctionnait pas correctement. En analysant les données, nous avons constaté que le champ `tag` peut lui-même contenir des virgules (ex : `"dark, gritty"`). Pour limiter ce problème, nous avons utilisé `split(",", 3)` au lieu de `split(",")`, ce qui plafonne le découpage à 3 séparateurs et évite de couper le tag en trop de morceaux dans la majorité des cas. Cette approche est suffisante pour ce dataset, même si elle ne constitue pas un parseur CSV complet.

Nous avons également encapsulé l'ensemble des instructions dans un bloc `try ... except` conformément aux consignes, pour ignorer les lignes malformées du fichier.

Le script a d'abord été validé sur l'échantillon (test local avec `mrjob` en mode inline), puis exécuté sur le fichier complet dans HDFS avec la configuration par défaut de Hadoop.

### Question 2 — Tags par utilisateur

La démarche est identique à la question 1, mais le mapper émet `(userId, 1)` au lieu de `(movieId, 1)`. Le reducer additionne les occurrences par `userId` pour obtenir le nombre total de tags ajoutés par chaque utilisateur. Le même soin a été apporté à l'utilisation de `split(",", 3)` et du `try ... except`.

### Question 3 — Nombre de blocs HDFS

Cette question ne nécessite pas de script MapReduce. Nous avons utilisé les commandes HDFS pour interroger directement les métadonnées du fichier.

La taille du fichier `tags.csv` est de **38 810 332 octets** (environ 37 Mo).

- Avec la configuration par défaut (taille de bloc = 128 Mo) : le fichier tient dans **1 seul bloc**.
- Avec une taille de bloc réduite à 64 Mo : le fichier tient également dans **1 seul bloc** (37 Mo < 64 Mo).

Dans les deux cas, le fichier est suffisamment petit pour tenir dans un seul bloc HDFS, quelle que soit la configuration testée.

### Question 4 — Occurrences de chaque tag

Le mapper émet `(tag, 1)` pour chaque ligne valide. Le reducer somme les occurrences de chaque tag.

Un défi particulier sur cette question : les tags du fichier peuvent contenir des espaces (ex : `dark comedy`, `great dialogue`, `artificial intelligence`). La conversion naïve avec `awk` en utilisant `$1` et `$2` ne fonctionnait pas car `awk` découpe sur les espaces, ce qui coupait les tags en plusieurs colonnes et produisait une sortie incorrecte (colonne `tag` vide, reste du tag dans la colonne `tagCount`).

Nous avons donc utilisé un petit script Python pour la conversion, qui découpe à la **dernière tabulation** (`rsplit('\t', 1)`), préservant ainsi correctement les tags avec des espaces dans la colonne `tag`.

Les jobs ont été lancés deux fois : une fois avec le fichier `tags.csv` (configuration par défaut) et une fois avec `tags_64mo.csv` (fichier uploadé avec une taille de bloc forcée à 64 Mo).

### Question 5 — Tags par utilisateur pour chaque film

Le mapper émet la clé composée `movieId + "," + userId` avec la valeur `1`. Le reducer somme les occurrences de chaque paire `(movieId, userId)`, ce qui donne le nombre de tags que chaque utilisateur a introduits pour chaque film.

Les jobs ont été lancés sur les deux configurations (défaut et 64 Mo), sur les fichiers `tags.csv` et `tags_64mo.csv` respectivement.

---

## 3. Résultats obtenus

### Question 1 — Combien de tags chaque film possède-t-il ?

```
movieId,tagCount
1,697
10,137
100,18
1000,10
100001,1
100003,3
100008,9
100017,9
100032,2
```

Le fichier résultat complet contient **45 251 lignes**, correspondant à 45 251 films distincts ayant au moins un tag dans le dataset.

Résultats complets : [q1_tags_per_movie_full.csv](https://github.com/AnissaSD/AKROUH_LUCAS-Big-Data/blob/main/R%C3%A9sultats/result_tags_per_movie%20(1).txt)

---

### Question 2 — Combien de tags chaque utilisateur a-t-il ajoutés ?

```
userId,tagCount
100001,9
100016,50
100028,4
100029,1
100033,1
100046,133
100051,19
100058,5
100065,2
```

Le fichier résultat complet contient **14 593 lignes**
Résultats complets : [result_tags_per_user.csv](https://github.com/AnissaSD/AKROUH_LUCAS-Big-Data/blob/main/R%C3%A9sultats/result_tags_per_user%20(1).txt)

---

### Question 3 — Combien de blocs le fichier occupe-t-il dans HDFS ?

```
[maria_dev@sandbox-hdp ~]$ hdfs dfs -du /datasets/exam/tags.csv
 38810332 /datasets/exam/tags.csv

[maria_dev@sandbox-hdp ~]$ hdfs getconf -confKey dfs.blocksize
 134217728

[maria_dev@sandbox-hdp ~]$ hdfs dfs -stat "%b %o %n" /datasets/exam/tags.csv
 38810332 134217728 tags.csv

[maria_dev@sandbox-hdp ~]$ python -c "import math; print(math.ceil(38810332 / 134217728.0))"
 1

[maria_dev@sandbox-hdp ~]$ python -c "import math; print(math.ceil(38810332 / 67108864.0))"
 1
```

**Résultat :** Le fichier `tags.csv` pèse environ **37 Mo**. Qu'on utilise la configuration par défaut (bloc de 128 Mo) ou une taille de bloc réduite à 64 Mo, le fichier occupe dans les deux cas **1 seul bloc** dans HDFS, car 37 Mo est inférieur à 64 Mo.

---

### Question 4 — Combien de fois chaque tag a-t-il été utilisé ?

Configuration par défaut :

```
tag,tagCount
Alexander Skarsgård,1
Difficile à trouver,1
Filmes Antigos ,2
Filmes Antigos,2
Kartik Aaryan,1
Kriti Sanon,1
Canyon des Lauriers,1
Luis Brandoni,1
Masami Nagasawa,1
```

> **Note :** Certaines entrées semblent proches visuellement (ex. `Filmes Antigos ` avec espace final et `Filmes Antigos` sans espace). Il s'agit de tags distincts dans les données brutes : les utilisateurs ont saisi ces tags avec des variantes d'écriture (espace en fin de chaîne, casse différente, etc.). Le script ne normalise pas les tags, il les restitue tels quels depuis le fichier source.

Configuration bloc 64 Mo :

```
tag,tagCount
Alexander Skarsgård,1
Difficile à trouver,1
Filmes Antigos ,2
Filmes Antigos,2
Kartik Aaryan,1
Kriti Sanon,1
Canyon des Lauriers,1
Luis Brandoni,1
Masami Nagasawa,1
```
Le fichier résultat complet contient **73 017 lignes**

Les résultats sont identiques dans les deux configurations (attendu, car le contenu du fichier ne change pas).

Résultats complets (config défaut) : [result_tag_count.csv](https://github.com/AnissaSD/AKROUH_LUCAS-Big-Data/blob/main/R%C3%A9sultats/result_tag_count%20(1).txt)

Résultats complets (config 64 Mo) : [result_tag_count_64.csv](https://github.com/AnissaSD/AKROUH_LUCAS-Big-Data/blob/main/R%C3%A9sultats/result_tag_count_64%20(1).txt)

---

### Question 5 — Pour chaque film, combien de tags le même utilisateur a-t-il introduits ?

Configuration par défaut :

```
"1,100538"	4
"1,10231"	2
"1,102568"	4
"1,102901"	1
"1,103368"	1
"1,103371"	1
"1,103883"	3
"1,104394"	9
"1,1048"	1
"1,105717"	1
```

> **Note sur le format :** Chaque ligne contient la clé `"movieId,userId"` séparée par une tabulation du comptage. Les clés sont encadrées de guillemets par `mrjob`. Ici par exemple, `"1,100538"\t4` signifie que l'utilisateur 100538 a introduit 4 tags pour le film 1.

Configuration bloc 64 Mo :

```
"1,100538"	4
"1,10231"	2
"1,102568"	4
"1,102901"	1
"1,103368"	1
"1,103371"	1
"1,103883"	3
"1,104394"	9
"1,1048"	1
"1,105717"	1
```
Le fichier résultat complet contient **305 356 lignes**

Les résultats sont identiques dans les deux configurations.

Résultats complets (config défaut) : [result_user_tags_per_movie_default.txt](https://github.com/AnissaSD/AKROUH_LUCAS-Big-Data/blob/main/R%C3%A9sultats/result_user_tags_per_movie_default.txt)

Résultats complets (config 64 Mo) : [result_user_tags_per_movie.txt](https://github.com/AnissaSD/AKROUH_LUCAS-Big-Data/blob/main/R%C3%A9sultats/result_user_tags_per_movie.txt)
