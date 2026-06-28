# DSML 2026 — Εργασία Big Data (dsml00289)

**Φοιτητής:** Κερκύρα Δημησιάνου  
**Α.Μ.:** dsml00289  
**Μάθημα:** Διαχείριση Δεδομένων Μεγάλης Κλίμακας  
**Περιβάλλον:** Apache Spark 3.5.8, Kubernetes (lab cluster), HDFS

**GitHub:** https://github.com/kerkyradim/bigdata-dsml  
Πλήρης αναφορά: [`report/REPORT.md`](report/REPORT.md)

## Δομή αποθετηρίου

```text
dsml00289-bigdata/
├── README.md                 # αυτό το αρχείο
├── LLM_USAGE.md              # δήλωση χρήσης LLM
├── report/
│   └── REPORT.md             # αναφορά (Queries 1–3 + CSV/Parquet)
├── code/                     # PySpark scripts
│   ├── Q1_df.py
│   ├── Q1_df_udf.py
│   ├── Q1_rdd.py
│   ├── Q1_convert_parquet.py
│   ├── Q1_df_parquet.py
│   ├── Q2_df.py
│   ├── Q2_sql.py
│   ├── Q3_df.py
│   ├── Q3_rdd.py
│   └── Q3_df_joins.py
└── results/
    ├── Q1_times.txt          # συνοπτικοί χρόνοι Q1
    ├── Q1_format_times.txt   # CSV vs Parquet
    ├── Q2_times.txt
    ├── Q3_times.txt
    ├── Q3_join_times.txt
    └── logs/                 # spark-submit / driver logs
```

## Προαπαιτούμενα

- WSL Ubuntu με ρυθμισμένο `~/bigdata-env.sh`
- Πρόσβαση στο Kubernetes cluster του εργαστηρίου (`dsml00289-priv`)
- Δεδομένα στο HDFS κάτω από `/data/` (course dataset)

## Εκτέλεση (παράδειγμα Query 1)

```bash
source ~/bigdata-env.sh
cd ~/dsml00289-bigdata   

spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  code/Q1_df.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289
```

Έλεγχος αποτελεσμάτων από driver pod:

```bash
kubectl -n dsml00289-priv get pods | grep q1-
kubectl -n dsml00289-priv logs <driver-pod-name> | tail -n 15
```

## Scripts ανά query

| Query | Αρχεία | Σημειώσεις |
|---|---|---|
| Q1 (DF / UDF / RDD) | `Q1_df.py`, `Q1_df_udf.py`, `Q1_rdd.py` | Benchmark: 2 exec × 1c × 2g |
| CSV → Parquet | `Q1_convert_parquet.py` | One-time μετατροπή |
| Q1 Parquet | `Q1_df_parquet.py` | Ίδια λογική με `Q1_df.py` |
| Q2 (DF / SQL) | `Q2_df.py`, `Q2_sql.py` | Benchmark: 4 exec × 1c × 2g |
| Q3 (DF / RDD / joins) | `Q3_df.py`, `Q3_rdd.py`, `Q3_df_joins.py` | Benchmark: 3 exec × 1c × 2g |

## Αναπαραγωγιμότητα

Οι χρόνοι εκτέλεσης (`QUERY_ELAPSED_SECONDS`) καταγράφονται στα `results/*_times.txt` και στα αντίστοιχα logs κάτω από `results/logs/`.

Namespace Kubernetes: **`dsml00289-priv`**
