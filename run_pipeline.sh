#!/bin/bash
# =============================================================
# run_pipeline.sh — VERSION WINDOWS/Git Bash corrigée
# Fix : MSYS_NO_PATHCONV=1 + chemin spark hardcodé dans container
# =============================================================

CONTAINER="spark-master"
REMOTE_DIR="/tmp/projet_cleaning"
SCRIPTS_DIR="$(dirname "$0")"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║     PFE Big Data - Sujet 3 : Data Cleaning Pipeline     ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Vérifier que les containers tournent
echo "Vérification des containers..."
for c in namenode datanode1 datanode2 spark-master spark-worker; do
    STATUS=$(docker inspect -f '{{.State.Running}}' $c 2>/dev/null)
    if [ "$STATUS" = "true" ]; then
        echo "  ✔  $c"
    else
        echo "  ✗  $c  — ARRÊTÉ (lancer: docker-compose up -d)"
        exit 1
    fi
done
echo ""

# Détecter le chemin Spark dans le container
echo "Détection du chemin Spark dans le container..."
SPARK_HOME_CONTAINER=$(docker exec $CONTAINER bash -c 'echo $SPARK_HOME' 2>/dev/null)
if [ -z "$SPARK_HOME_CONTAINER" ]; then
    SPARK_HOME_CONTAINER="/spark"
fi
SPARK_SUBMIT="${SPARK_HOME_CONTAINER}/bin/spark-submit"
echo "  → spark-submit : $SPARK_SUBMIT"
echo ""

# Créer le répertoire distant et copier les scripts
echo "Copie des scripts dans le container $CONTAINER..."
MSYS_NO_PATHCONV=1 docker exec $CONTAINER mkdir -p $REMOTE_DIR

# Copier chaque script avec chemin absolu Windows-safe
CURRENT_DIR=$(pwd -W 2>/dev/null || pwd)
for script in 01_profiling.py 02_cleaning.py 03_impact_analysis.py; do
    docker cp "${CURRENT_DIR}/${script}" $CONTAINER:${REMOTE_DIR}/${script}
    if [ $? -eq 0 ]; then
        echo "  ✔  $script"
    else
        echo "  ✗  $script — erreur de copie"
    fi
done
echo ""

# ---------------------------------------------------------------
# ÉTAPE 0 : Setup HDFS
# ---------------------------------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " ÉTAPE 0 : Upload dataset → HDFS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
bash "${SCRIPTS_DIR}/00_setup_hdfs.sh"
echo ""

# Fonction pour soumettre un job Spark
# MSYS_NO_PATHCONV=1 empêche Git Bash de convertir les chemins Linux en chemins Windows
submit_spark_job() {
    local script=$1
    MSYS_NO_PATHCONV=1 docker exec $CONTAINER \
        $SPARK_SUBMIT \
        --master spark://spark-master:7077 \
        --executor-memory 1g \
        --total-executor-cores 1 \
        ${REMOTE_DIR}/${script}
}

# ---------------------------------------------------------------
# ÉTAPE 1 : Profilage
# ---------------------------------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " ÉTAPE 1 : Profilage statistique"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
submit_spark_job "01_profiling.py"
echo ""

# ---------------------------------------------------------------
# ÉTAPE 2 : Nettoyage
# ---------------------------------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " ÉTAPE 2 : Nettoyage (règles métier + Z-Score)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
submit_spark_job "02_cleaning.py"
echo ""

# ---------------------------------------------------------------
# ÉTAPE 3 : Analyse d'impact
# ---------------------------------------------------------------
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " ÉTAPE 3 : Analyse d'impact avant/après"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
submit_spark_job "03_impact_analysis.py"
echo ""

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  PIPELINE TERMINÉ                                       ║"
echo "║  Interfaces disponibles :                               ║"
echo "║    Hadoop NameNode  →  http://localhost:9870            ║"
echo "║    Spark Master UI  →  http://localhost:8080            ║"
echo "╚══════════════════════════════════════════════════════════╝"