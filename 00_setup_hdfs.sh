#!/bin/bash
# =============================================================
# 00_setup_hdfs.sh  — VERSION WINDOWS/Git Bash corrigée
# MSYS_NO_PATHCONV=1 bloque la conversion automatique des chemins
# =============================================================

DATASET_LOCAL="yellow_tripdata_2023-01.parquet"
CONTAINER="namenode"

echo "========================================"
echo " SETUP HDFS - Sujet 3 Data Cleaning"
echo "========================================"

# Vérifier que le dataset existe dans le dossier courant
if [ ! -f "$DATASET_LOCAL" ]; then
    echo "ERREUR : '$DATASET_LOCAL' introuvable dans le dossier courant"
    echo "Placez-vous dans le même dossier que le dataset avec : cd <dossier>"
    exit 1
fi

echo "[1/4] Création du répertoire HDFS /data/taxi ..."
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -mkdir -p /data/taxi
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -chmod 777 /data/taxi

echo "[2/4] Copie du dataset dans le container..."
# Utiliser winpty + chemin absolu pour éviter la conversion Git Bash
CURRENT_DIR=$(pwd -W 2>/dev/null || pwd)
docker cp "${CURRENT_DIR}/${DATASET_LOCAL}" $CONTAINER:/tmp/yellow_tripdata_2023-01.parquet

if [ $? -ne 0 ]; then
    echo "  Tentative avec chemin Unix..."
    docker cp "./${DATASET_LOCAL}" $CONTAINER:/tmp/yellow_tripdata_2023-01.parquet
fi

echo "[3/4] Upload vers HDFS..."
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -put -f /tmp/yellow_tripdata_2023-01.parquet /data/taxi/

echo "[4/4] Vérification..."
MSYS_NO_PATHCONV=1 docker exec $CONTAINER hdfs dfs -ls -h /data/taxi/

echo ""
docker exec $CONTAINER hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|DFS Used:|DFS Remaining" | head -5

echo ""
echo "✔  hdfs://namenode:9000/data/taxi/yellow_tripdata_2023-01.parquet"