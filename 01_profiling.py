# =============================================================
# 01_profiling.py
# Phase 2 : Profilage statistique du dataset NYC Taxi
# Calcule toutes les métriques en UNE SEULE passe Spark
# Spark 3.1.1 - Hadoop 3.2.1
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when, isnan, percentile_approx
import time

# ------------------------------------------------------------------
# 1. INITIALISATION SPARK
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("NYC_Taxi_Profiling") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  PHASE 2 : PROFILAGE STATISTIQUE - NYC Yellow Taxi 2023")
print("=" * 60)
print(f"  Spark version : {spark.version}")
print(f"  FS par défaut : hdfs://namenode:9000")
print()

HDFS_PATH = "hdfs://namenode:9000/data/taxi/yellow_tripdata_2023-01.parquet"

# ------------------------------------------------------------------
# 2. CHARGEMENT DU DATASET
# ------------------------------------------------------------------
print("[1/5] Chargement du dataset depuis HDFS...")
t0 = time.time()
df = spark.read.parquet(HDFS_PATH)
nb_lignes = df.count()
nb_colonnes = len(df.columns)
t_load = time.time() - t0

print(f"  → {nb_lignes:,} lignes  |  {nb_colonnes} colonnes  |  chargé en {t_load:.2f}s")
print()

# Schéma
print("[2/5] Schéma du dataset :")
df.printSchema()

# ------------------------------------------------------------------
# 3. COLONNES CRITIQUES POUR LE NETTOYAGE
# ------------------------------------------------------------------
COLS_CRITIQUES = [
    "fare_amount",
    "trip_distance",
    "total_amount",
    "tip_amount",
    "passenger_count",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# Garder seulement les colonnes qui existent réellement
COLS_CRITIQUES = [c for c in COLS_CRITIQUES if c in df.columns]
COLS_NUMERIQUES = [c for c in ["fare_amount", "trip_distance", "total_amount",
                                "tip_amount", "passenger_count"] if c in df.columns]

# ------------------------------------------------------------------
# 4. STATISTIQUES DE BASE (une seule passe avec .agg())
# ------------------------------------------------------------------
print("[3/5] Calcul des statistiques (µ, σ, min, max, quartiles)...")
t0 = time.time()

# Construire les agrégations dynamiquement
agg_exprs = []
for c in COLS_NUMERIQUES:
    agg_exprs += [
        F.mean(col(c)).alias(f"{c}_mean"),
        F.stddev(col(c)).alias(f"{c}_stddev"),
        F.min(col(c)).alias(f"{c}_min"),
        F.max(col(c)).alias(f"{c}_max"),
        F.count(when(col(c).isNull() | isnan(col(c)), 1)).alias(f"{c}_nulls"),
    ]

stats_row = df.agg(*agg_exprs).collect()[0]

# Quartiles (approxQuantile est une action séparée)
quartiles = {}
for c in COLS_NUMERIQUES:
    try:
        q = df.approxQuantile(c, [0.25, 0.50, 0.75], 0.01)
        quartiles[c] = {"Q1": q[0], "median": q[1], "Q3": q[2]}
    except Exception:
        quartiles[c] = {"Q1": None, "median": None, "Q3": None}

t_stats = time.time() - t0
print(f"  → Stats calculées en {t_stats:.2f}s")
print()

# ------------------------------------------------------------------
# 5. AFFICHAGE DU TABLEAU "ÉTAT AVANT NETTOYAGE"
# ------------------------------------------------------------------
print("[4/5] ═══ TABLEAU : ÉTAT AVANT NETTOYAGE ═══")
print()
header = f"{'Colonne':<20} {'Moyenne':>10} {'Écart-type':>12} {'Min':>10} {'Max':>12} {'Q1':>10} {'Médiane':>10} {'Q3':>10} {'Nulls':>8} {'% Nulls':>8}"
print(header)
print("─" * len(header))

for c in COLS_NUMERIQUES:
    mean_v   = stats_row[f"{c}_mean"]
    std_v    = stats_row[f"{c}_stddev"]
    min_v    = stats_row[f"{c}_min"]
    max_v    = stats_row[f"{c}_max"]
    nulls_v  = stats_row[f"{c}_nulls"]
    q1_v     = quartiles[c]["Q1"]
    med_v    = quartiles[c]["median"]
    q3_v     = quartiles[c]["Q3"]
    pct_null = (nulls_v / nb_lignes * 100) if nb_lignes > 0 else 0

    def fmt(v): return f"{v:>10.2f}" if v is not None else f"{'N/A':>10}"

    print(f"{c:<20} {fmt(mean_v)} {fmt(std_v)} {fmt(min_v)} {fmt(max_v)} "
          f"{fmt(q1_v)} {fmt(med_v)} {fmt(q3_v)} {nulls_v:>8,} {pct_null:>7.2f}%")

print()

# ------------------------------------------------------------------
# 6. ANALYSE DES NULLS ET DOUBLONS
# ------------------------------------------------------------------
print("[5/5] Analyse globale des nulls et doublons...")

# Nulls sur toutes les colonnes en une passe
null_counts = df.select([
    count(when(col(c).isNull(), 1)).alias(c)
    for c in df.columns
]).collect()[0]

print("\n  Valeurs nulles par colonne :")
cols_avec_nulls = {c: null_counts[c] for c in df.columns if null_counts[c] > 0}
if cols_avec_nulls:
    for c, n in sorted(cols_avec_nulls.items(), key=lambda x: -x[1]):
        print(f"    {c:<30} {n:>8,}  ({n/nb_lignes*100:.2f}%)")
else:
    print("    Aucune valeur nulle détectée")

# Doublons
nb_distincts = df.dropDuplicates().count()
nb_doublons  = nb_lignes - nb_distincts
print(f"\n  Doublons : {nb_doublons:,} lignes dupliquées ({nb_doublons/nb_lignes*100:.3f}%)")

# ------------------------------------------------------------------
# 7. RÉSUMÉ FINAL
# ------------------------------------------------------------------
print()
print("═" * 60)
print("  RÉSUMÉ DU PROFILAGE")
print("═" * 60)
print(f"  Lignes totales          : {nb_lignes:>12,}")
print(f"  Colonnes                : {nb_colonnes:>12}")
print(f"  Lignes dupliquées       : {nb_doublons:>12,}")
print(f"  Colonnes avec nulls     : {len(cols_avec_nulls):>12}")

# Signaux d'alerte
print()
print("  Alertes détectées :")
if "fare_amount" in COLS_NUMERIQUES:
    fare_min = stats_row["fare_amount_min"]
    fare_max = stats_row["fare_amount_max"]
    if fare_min is not None and fare_min < 0:
        print(f"    ⚠  fare_amount MIN = {fare_min:.2f}  → valeurs négatives présentes")
    if fare_max is not None and fare_max > 500:
        print(f"    ⚠  fare_amount MAX = {fare_max:.2f}  → outliers extrêmes possibles")

if "trip_distance" in COLS_NUMERIQUES:
    dist_max = stats_row["trip_distance_max"]
    if dist_max is not None and dist_max > 200:
        print(f"    ⚠  trip_distance MAX = {dist_max:.2f} miles → vérifier vitesse taxi")

if "passenger_count" in COLS_NUMERIQUES:
    pass_min = stats_row["passenger_count_min"]
    if pass_min is not None and pass_min <= 0:
        print(f"    ⚠  passenger_count MIN = {pass_min:.0f}  → trajets sans passager")

print()
print("  → Sauvegarder ce tableau : c'est votre référence 'AVANT nettoyage'")
print("  → Prochain script : 02_cleaning.py")
print()

spark.stop()