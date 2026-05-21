# =============================================================
# 02_cleaning.py
# Phase 3 : Moteur de validation et nettoyage distribué
# Z-Score + règles métier + imputation + sauvegarde
# Spark 3.1.1 - Hadoop 3.2.1
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, isnan, unix_timestamp, abs as spark_abs
import time

# ------------------------------------------------------------------
# 1. INITIALISATION SPARK
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("NYC_Taxi_Cleaning") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  PHASE 3 : NETTOYAGE DISTRIBUÉ - NYC Yellow Taxi 2023")
print("=" * 60)
print()

HDFS_INPUT  = "hdfs://namenode:9000/data/taxi/yellow_tripdata_2023-01.parquet"
HDFS_OUTPUT = "hdfs://namenode:9000/data/taxi/yellow_taxi_clean"

# ------------------------------------------------------------------
# 2. CHARGEMENT
# ------------------------------------------------------------------
print("[1/6] Chargement du dataset brut...")
t0 = time.time()
df_raw = spark.read.parquet(HDFS_INPUT)
nb_initial = df_raw.count()
print(f"  → {nb_initial:,} lignes chargées en {time.time()-t0:.2f}s")
print()

# Compteur de suivi des rejets
rejets = {}

# ------------------------------------------------------------------
# 3. ÉTAPE A — SUPPRESSION DES DOUBLONS
# ------------------------------------------------------------------
print("[2/6] Étape A : Suppression des doublons...")
df = df_raw.dropDuplicates()
nb_apres_dedup = df.count()
rejets["doublons"] = nb_initial - nb_apres_dedup
print(f"  → {rejets['doublons']:,} doublons supprimés")
print()

# ------------------------------------------------------------------
# 4. ÉTAPE B — RÈGLES MÉTIER (filtres logiques)
# ------------------------------------------------------------------
print("[3/6] Étape B : Règles métier (incohérences logiques)...")

nb_avant_metier = df.count()

# Règle 1 : fare_amount doit être >= 2.50 (tarif minimum NYC) et <= 1000
df = df.filter(col("fare_amount").isNotNull() &
               (col("fare_amount") >= 2.5) &
               (col("fare_amount") <= 1000))

# Règle 2 : trip_distance > 0 et < 200 miles (impossible physiquement en taxi NYC)
df = df.filter(col("trip_distance").isNotNull() &
               (col("trip_distance") > 0) &
               (col("trip_distance") < 200))

# Règle 3 : passenger_count entre 1 et 8
df = df.filter(col("passenger_count").isNotNull() &
               (col("passenger_count") >= 1) &
               (col("passenger_count") <= 8))

# Règle 4 : total_amount > 0
df = df.filter(col("total_amount").isNotNull() &
               (col("total_amount") > 0))

# Règle 5 : durée de trajet cohérente (pickup < dropoff, et durée < 5h)
if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
    df = df.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("tpep_dropoff_datetime").isNotNull() &
        (unix_timestamp("tpep_dropoff_datetime") > unix_timestamp("tpep_pickup_datetime")) &
        ((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) < 5*3600)
    )

# Règle 6 : vitesse moyenne <= 150 km/h (taxi NYC, pas un avion)
if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
    df = df.withColumn(
        "duree_heures",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600.0
    )
    df = df.filter(
        (col("duree_heures") > 0) &
        ((col("trip_distance") * 1.60934 / col("duree_heures")) <= 150)
    )
    df = df.drop("duree_heures")

nb_apres_metier = df.count()
rejets["regles_metier"] = nb_avant_metier - nb_apres_metier
print(f"  → {rejets['regles_metier']:,} lignes rejetées par les règles métier")
print()

# ------------------------------------------------------------------
# 5. ÉTAPE C — Z-SCORE (détection d'outliers statistiques)
# ------------------------------------------------------------------
print("[4/6] Étape C : Détection d'outliers par Z-Score (seuil |Z| > 3)...")

# Calculer µ et σ UNE SEULE FOIS sur le dataset après règles métier
stats = df.select(
    F.mean("fare_amount").alias("mu_fare"),
    F.stddev("fare_amount").alias("sigma_fare"),
    F.mean("trip_distance").alias("mu_dist"),
    F.stddev("trip_distance").alias("sigma_dist"),
    F.mean("total_amount").alias("mu_total"),
    F.stddev("total_amount").alias("sigma_total"),
).collect()[0]

mu_fare    = stats["mu_fare"]
sigma_fare = stats["sigma_fare"]
mu_dist    = stats["mu_dist"]
sigma_dist = stats["sigma_dist"]
mu_total   = stats["mu_total"]
sigma_total = stats["sigma_total"]

print(f"  µ fare_amount  = {mu_fare:.4f}  |  σ = {sigma_fare:.4f}")
print(f"  µ trip_distance = {mu_dist:.4f}  |  σ = {sigma_dist:.4f}")
print(f"  µ total_amount = {mu_total:.4f}  |  σ = {sigma_total:.4f}")
print()

# Ajouter les colonnes Z-Score (pour analyse, elles seront retirées après)
SEUIL_Z = 3.0

df_avec_z = df \
    .withColumn("z_fare",
        spark_abs((col("fare_amount") - mu_fare) / sigma_fare)) \
    .withColumn("z_dist",
        spark_abs((col("trip_distance") - mu_dist) / sigma_dist)) \
    .withColumn("z_total",
        spark_abs((col("total_amount") - mu_total) / sigma_total))

# Compter les outliers AVANT suppression (pour le rapport)
nb_outliers_fare  = df_avec_z.filter(col("z_fare")  > SEUIL_Z).count()
nb_outliers_dist  = df_avec_z.filter(col("z_dist")  > SEUIL_Z).count()
nb_outliers_total = df_avec_z.filter(col("z_total") > SEUIL_Z).count()

print(f"  Outliers détectés (|Z| > {SEUIL_Z}) :")
print(f"    fare_amount   : {nb_outliers_fare:>8,} lignes")
print(f"    trip_distance : {nb_outliers_dist:>8,} lignes")
print(f"    total_amount  : {nb_outliers_total:>8,} lignes")
print()

# Filtrer : garder uniquement les lignes avec TOUS les Z <= seuil
nb_avant_zscore = df.count()
df_clean = df_avec_z.filter(
    (col("z_fare")  <= SEUIL_Z) &
    (col("z_dist")  <= SEUIL_Z) &
    (col("z_total") <= SEUIL_Z)
).drop("z_fare", "z_dist", "z_total")

nb_apres_zscore = df_clean.count()
rejets["zscore"] = nb_avant_zscore - nb_apres_zscore
print(f"  → {rejets['zscore']:,} lignes supprimées par Z-Score")
print()

# ------------------------------------------------------------------
# 6. ÉTAPE D — IMPUTATION DES VALEURS MANQUANTES RÉSIDUELLES
# ------------------------------------------------------------------
print("[5/6] Étape D : Imputation des valeurs manquantes résiduelles...")

# Calculer les médianes pour l'imputation
medianes = df_clean.approxQuantile(
    ["tip_amount", "passenger_count"],
    [0.5], 0.01
)
med_tip  = medianes[0][0] if medianes[0] else 0.0
med_pass = medianes[1][0] if len(medianes) > 1 and medianes[1] else 1.0

df_clean = df_clean.fillna({
    "tip_amount": med_tip,
    "passenger_count": int(med_pass)
})

print(f"  → Imputation tip_amount avec médiane : {med_tip:.2f}")
print(f"  → Imputation passenger_count avec médiane : {int(med_pass)}")
print()

# ------------------------------------------------------------------
# 7. SAUVEGARDE DU DATASET PROPRE
# ------------------------------------------------------------------
print("[6/6] Sauvegarde du dataset propre en Parquet...")
t0 = time.time()
df_clean.write \
    .mode("overwrite") \
    .parquet(HDFS_OUTPUT)
t_write = time.time() - t0

nb_final = df_clean.count()
print(f"  → {nb_final:,} lignes sauvegardées en {t_write:.2f}s")
print(f"  → Chemin HDFS : {HDFS_OUTPUT}")
print()

# ------------------------------------------------------------------
# 8. BILAN COMPLET DES REJETS
# ------------------------------------------------------------------
nb_total_rejets = nb_initial - nb_final
taux_rejet = (nb_total_rejets / nb_initial) * 100

print("═" * 60)
print("  BILAN DES REJETS")
print("═" * 60)
print(f"  Lignes initiales         : {nb_initial:>12,}")
print(f"  Doublons supprimés       : {rejets['doublons']:>12,}  ({rejets['doublons']/nb_initial*100:>6.3f}%)")
print(f"  Rejets règles métier     : {rejets['regles_metier']:>12,}  ({rejets['regles_metier']/nb_initial*100:>6.3f}%)")
print(f"  Rejets Z-Score           : {rejets['zscore']:>12,}  ({rejets['zscore']/nb_initial*100:>6.3f}%)")
print(f"  ─────────────────────────────────────────")
print(f"  Total rejeté             : {nb_total_rejets:>12,}  ({taux_rejet:>6.3f}%)")
print(f"  Lignes finales (propres) : {nb_final:>12,}")
print()
print("  → Dataset prêt pour 03_impact_analysis.py")
print()

spark.stop()