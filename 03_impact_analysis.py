# =============================================================
# 03_impact_analysis.py
# Phase 4 : Quantification de l'impact du nettoyage
# Comparaison avant/après + analyse du biais de sélection
# Spark 3.1.1 - Hadoop 3.2.1
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, hour, dayofweek, month
import time

# ------------------------------------------------------------------
# 1. INITIALISATION SPARK
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("NYC_Taxi_Impact_Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  PHASE 4 : ANALYSE D'IMPACT DU NETTOYAGE")
print("=" * 60)
print()

HDFS_RAW   = "hdfs://namenode:9000/data/taxi/yellow_tripdata_2023-01.parquet"
HDFS_CLEAN = "hdfs://namenode:9000/data/taxi/yellow_taxi_clean"

# ------------------------------------------------------------------
# 2. CHARGEMENT DES DEUX DATASETS
# ------------------------------------------------------------------
print("[1/4] Chargement des datasets brut et propre...")
df_raw   = spark.read.parquet(HDFS_RAW)
df_clean = spark.read.parquet(HDFS_CLEAN)

nb_raw   = df_raw.count()
nb_clean = df_clean.count()
print(f"  Brut   : {nb_raw:,} lignes")
print(f"  Propre : {nb_clean:,} lignes")
print(f"  Rejeté : {nb_raw - nb_clean:,} lignes  ({(nb_raw - nb_clean)/nb_raw*100:.3f}%)")
print()

# ------------------------------------------------------------------
# 3. TABLEAU COMPARATIF AVANT / APRÈS
# ------------------------------------------------------------------
print("[2/4] ═══ TABLEAU COMPARATIF AVANT / APRÈS NETTOYAGE ═══")
print()

COLS_COMP = ["fare_amount", "trip_distance", "total_amount", "tip_amount"]
COLS_COMP = [c for c in COLS_COMP if c in df_raw.columns and c in df_clean.columns]

# Calculer les stats sur les deux datasets en une passe chacun
def get_stats(df, cols):
    aggs = []
    for c in cols:
        aggs += [
            F.mean(col(c)).alias(f"{c}_mean"),
            F.stddev(col(c)).alias(f"{c}_stddev"),
            F.min(col(c)).alias(f"{c}_min"),
            F.max(col(c)).alias(f"{c}_max"),
        ]
    return df.agg(*aggs).collect()[0]

stats_raw   = get_stats(df_raw,   COLS_COMP)
stats_clean = get_stats(df_clean, COLS_COMP)

# Affichage du tableau
print(f"{'Métrique':<22} {'Avant':>12} {'Après':>12} {'Erreur %':>10}")
print("─" * 60)

for c in COLS_COMP:
    mean_r = stats_raw[f"{c}_mean"]   or 0
    mean_c = stats_clean[f"{c}_mean"] or 0
    std_r  = stats_raw[f"{c}_stddev"]   or 0
    std_c  = stats_clean[f"{c}_stddev"] or 0
    min_r  = stats_raw[f"{c}_min"]   or 0
    min_c  = stats_clean[f"{c}_min"] or 0
    max_r  = stats_raw[f"{c}_max"]   or 0
    max_c  = stats_clean[f"{c}_max"] or 0

    err_mean = abs(mean_r - mean_c) / abs(mean_r) * 100 if mean_r != 0 else 0
    err_std  = abs(std_r  - std_c)  / abs(std_r)  * 100 if std_r  != 0 else 0

    print(f"\n  {c}")
    print(f"    {'Moyenne':<18} {mean_r:>12.4f} {mean_c:>12.4f} {err_mean:>9.2f}%")
    print(f"    {'Écart-type':<18} {std_r:>12.4f}  {std_c:>12.4f} {err_std:>9.2f}%")
    print(f"    {'Min':<18} {min_r:>12.4f} {min_c:>12.4f}")
    print(f"    {'Max':<18} {max_r:>12.4f} {max_c:>12.4f}")

print()
print("  Interprétation : L'erreur % sur la moyenne est l'écart induit")
print("  par la non-qualité ('Garbage In, Garbage Out')")
print()

# ------------------------------------------------------------------
# 4. ANALYSE DU BIAIS DE SÉLECTION
# ------------------------------------------------------------------
print("[3/4] ═══ ANALYSE DU BIAIS DE SÉLECTION ═══")
print()
print("  Question : les données rejetées sont-elles concentrées")
print("  sur certains VendorID, heures, ou jours ?")
print()

# Identifier les lignes rejetées
# Pour ça on marque le df_raw avec un flag et on joint avec df_clean
# Alternative plus simple : comparer les agrégats sur les deux datasets

# Distribution par VendorID
if "VendorID" in df_raw.columns:
    print("  Répartition par VendorID :")
    dist_raw = df_raw.groupBy("VendorID").count().withColumnRenamed("count", "nb_brut")
    dist_clean = df_clean.groupBy("VendorID").count().withColumnRenamed("count", "nb_propre")
    dist_join = dist_raw.join(dist_clean, "VendorID", "left") \
        .withColumn("taux_conservation",
            (col("nb_propre") / col("nb_brut") * 100).cast("double")) \
        .orderBy("VendorID")
    dist_join.show()

# Distribution par heure (biais horaire)
if "tpep_pickup_datetime" in df_raw.columns:
    print("  Taux de rejet par heure (top 5 heures avec le plus de rejets) :")
    df_raw_h   = df_raw.withColumn("heure", hour("tpep_pickup_datetime")) \
                       .groupBy("heure").count().withColumnRenamed("count", "nb_brut")
    df_clean_h = df_clean.withColumn("heure", hour("tpep_pickup_datetime")) \
                         .groupBy("heure").count().withColumnRenamed("count", "nb_propre")
    biais_h = df_raw_h.join(df_clean_h, "heure", "left") \
        .withColumn("rejets", col("nb_brut") - col("nb_propre")) \
        .withColumn("pct_rejet", (col("rejets") / col("nb_brut") * 100).cast("double")) \
        .orderBy(col("pct_rejet").desc())
    biais_h.show(5)

# Distribution mensuelle (vérification cohérence temporelle)
if "tpep_pickup_datetime" in df_raw.columns:
    print("  Trajets par mois (cohérence temporelle) :")
    df_clean.withColumn("mois", month("tpep_pickup_datetime")) \
            .groupBy("mois").count() \
            .orderBy("mois") \
            .show()

# ------------------------------------------------------------------
# 5. CONCLUSION ET LIEN MAURITANIEN
# ------------------------------------------------------------------
print("[4/4] ═══ SYNTHÈSE POUR LE RAPPORT ═══")
print()

taux_rejet_global = (nb_raw - nb_clean) / nb_raw * 100
mean_fare_avant = stats_raw["fare_amount_mean"]  or 0
mean_fare_apres = stats_clean["fare_amount_mean"] or 0
err_fare = abs(mean_fare_avant - mean_fare_apres) / abs(mean_fare_avant) * 100 if mean_fare_avant else 0
std_fare_avant = stats_raw["fare_amount_stddev"]  or 0
std_fare_apres = stats_clean["fare_amount_stddev"] or 0
reduction_std = (std_fare_avant - std_fare_apres) / std_fare_avant * 100 if std_fare_avant else 0

print(f"  Taux de rejet global        : {taux_rejet_global:.3f}%")
print(f"  Erreur sur µ(fare_amount)   : {err_fare:.2f}%")
print(f"  Réduction de σ (fare)       : {reduction_std:.2f}%")
print()
print("  ─── Analogie SMELEC (à inclure dans votre rapport) ───")
print()
print(f"  Sans nettoyage, la moyenne des recettes était {mean_fare_avant:.2f}$.")
print(f"  Après nettoyage, elle est {mean_fare_apres:.2f}$.")
print(f"  L'erreur induite par la non-qualité était de {err_fare:.2f}%.")
print()
print("  Transposé au contexte mauritanien :")
print("  Si SMELEC calcule la consommation moyenne d'un quartier")
print("  sans filtrer les compteurs défectueux (lectures > 5000 kWh/j),")
print(f"  les statistiques seraient faussées de ~{err_fare:.1f}%, entraînant")
print("  une surfacturation systématique et des réclamations coûteuses.")
print()
print("  C'est le principe 'Garbage In, Garbage Out' :")
print("  la décision ne vaut que ce que vaut la donnée qui l'alimente.")
print()
print("═" * 60)
print("  PIPELINE COMPLET TERMINÉ")
print("  Consultez Spark UI : http://localhost:4040")
print("═" * 60)

spark.stop()