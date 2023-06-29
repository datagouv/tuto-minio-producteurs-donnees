# Script d'upload de données dans un bucket Minio

Ce petit dépôt a pour objectif de donner les éléments aux producteurs de données devant publier les données via notre cluster Minio.

## Installation

```
pip install -r requirements.txt
```

Copy du fichier config_example.py

```
cp config_example.py config.py
```

Vous devez ensuite remplir les config que nous vous avons fournis par ailleurs

## Utilisation

```
python upload_file.py
```

Le fichier test.csv du dépôt est alors stocké sur le bucket spécifié.