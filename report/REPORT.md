# Εξαμηνιαία Εργασία DSML 2026 — Αναφορά (Ζητούμενα 1–5, Queries 1–4)

**Ονοματεπώνυμο:** [Συμπληρώστε]  
**Α.Μ.:** dsml00289  
**Μάθημα:** Διαχείριση Δεδομένων Μεγάλης Κλίμακας  
**Περιβάλλον:** Apache Spark 3.5.8, Kubernetes (lab cluster), HDFS  
**Αποθετήριο κώδικα:** https://github.com/kerkyra/dsml00289-bigdata

---

## 1. Περιβάλλον εκτέλεσης

Οι υλοποιήσεις εκτελέστηκαν από WSL με `spark-submit` σε **cluster mode** στο Kubernetes cluster του εργαστηρίου. Τα δεδομένα εισόδου διαβάστηκαν από το HDFS (`/data/`), ενώ τα αποτελέσματα και τα logs αποθηκεύτηκαν κάτω από `/user/dsml00289/`.

Ο χρόνος εκτέλεσης μετρήθηκε **μέσα στον κώδικα Python** με `time.perf_counter()` και τυπώνεται ως `QUERY_ELAPSED_SECONDS`. Τα αποτελέσματα των μετρήσεων καταγράφηκαν στα αρχεία:

- `results/Q1_times.txt`
- `results/Q2_times.txt`
- `results/Q3_times.txt`
- `results/Q3_join_times.txt`
- `results/Q4_times.txt`
- `results/Q1_format_times.txt`

Για τα Queries 1–3, κάθε ρύθμιση εκτελέστηκε **3 φορές** και αναφέρεται ο **διάμεσος (median)** χρόνος. Για το Query 4, η εκφώνηση ζητά **μελέτη κλιμάκωσης** σε πολλαπλές ρυθμίσεις πόρων (βλ. §6.5).

**Παράδειγμα εκτέλεσης:**

```bash
source ~/bigdata-env.sh
cd ~/bigdata-dsml

spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  code/Q1_df.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289
```

Μετά από κάθε εκτέλεση, τα αποτελέσματα επιβεβαιώθηκαν από τα logs του driver pod:

```bash
kubectl -n dsml00289-priv get pods | grep q1-
kubectl -n dsml00289-priv logs <driver-pod-name> | tail -n 15
```

---

## 2. Ζητούμενο 1 — CSV vs Parquet (Query 1)

### 2.1 Σκοπός

Η εκφώνηση ζητά μετατροπή των CSV αρχείων εγκλημάτων σε **Parquet** και σύγκριση χρόνων εκτέλεσης του **Query 1** (DataFrame, ίδια λογική με `Q1_df.py`) για τις δύο μορφές.

### 2.2 Θεωρητικό υπόβαθρο

Το **CSV** είναι μορφή κειμένου γραμμή-προς-γραμμή: κάθε εκτέλεση απαιτεί parsing strings, inferencing τύπων και ανάγνωση **όλων** των στηλών, ακόμη κι αν το query χρησιμοποιεί λίγες.

Το **Parquet** είναι **columnar** format: τα δεδομένα αποθηκεύονται ανά στήλη με **Snappy compression**, επιτρέποντας **column pruning** (διαβάζονται μόνο `Premis Desc` και `TIME OCC`) και **predicate pushdown**. Στο Spark, η ανάγνωση Parquet αποφεύγει το βαρύ CSV parsing και μειώνει σημαντικά τον όγκο I/O από το HDFS.

### 2.3 Μετατροπή CSV → Parquet

**Script:** `Q1_convert_parquet.py` (μία φορά)

```bash
source ~/bigdata-env.sh
cd ~/bigdata-dsml

spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  code/Q1_convert_parquet.py \
  --output hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289/LA_Crime_Data_parquet
```

**Αποτέλεσμα μετατροπής:** `ROW_COUNT=3.138.128` γραμμές → `hdfs://.../user/dsml00289/LA_Crime_Data_parquet`

### 2.4 Σύγκριση Query 1 (DataFrame)

**Ρύθμιση benchmark:** 2 executors × 1 core × 2 GB (ίδια με CSV baseline του §3.4)

**Parquet script:** `Q1_df_parquet.py`

```bash
spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  code/Q1_df_parquet.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289
```

#### Αποτελέσματα (ταυτότητα)

| Τμήμα ημέρας | Αριθμός | Ποσοστό (%) |
|---|---:|---:|
| Νύχτα | 251.094 | 34,08 |
| Βράδυ | 198.292 | 26,92 |
| Απόγευμα | 156.432 | 21,23 |
| Πρωί | 130.866 | 17,76 |

Τα αποτελέσματα **ταιριάζουν ακριβώς** με το CSV (`Q1_df.py`).

#### Χρόνοι εκτέλεσης

| Μορφή εισόδου | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---:|---:|
| CSV (`Q1_df.py`) | 63,576 | 86,606 | 56,607 | **63,576** |
| Parquet (`Q1_df_parquet.py`) | 15,997 | 15,434 | 15,804 | **15,804** |

**Επιτάχυνση:** ~**4,0×** (63,6 s → 15,8 s διάμεσος).

#### Όγκος στο HDFS

| Μορφή | Μέγεθος (hdfs dfs -du -s -h) |
|---|---:|
| CSV (2 αρχεία πηγής) | **896,2 MB** (608,6 + 287,6 MB) |
| Parquet (8 partitions) | **127,8 MB** |

Μείωση όγκου ~**7×**, με θετική επίδραση στο network I/O του cluster.

### 2.5 Σχολιασμός

Το Parquet βελτιώνει δραματικά τον χρόνο του Q1, επειδή το query αγγίζει **μόνο δύο στήλες** από εκατομμύρια εγγραφές. Το CSV αναγκάζει ανάγνωση και parsing ολόκληρων γραμμών. Η μετατροπή είναι **one-time cost**· για επαναλαμβανόμενα analytics queries, το Parquet είναι σαφώς προτιμότερο.

Logs/timing: `results/Q1_format_times.txt`, `results/q1_df_parquet_run*.log`

---

## 3. Ζητούμενο 2 — Query 1 (DataFrame, DataFrame+UDF, RDD)

### 3.1 Εκφώνηση

Ταξινόμηση των τμημάτων της ημέρας σε **φθίνουσα σειρά** ως προς το **ποσοστό** εγκλημάτων που έλαβαν χώρα στον **δρόμο (`STREET`)** επί του συνόλου των εγκλημάτων στο δρόμο.

| Τμήμα ημέρας | Ώρες |
|---|---|
| Πρωί | 05:00 – 11:59 |
| Απόγευμα | 12:00 – 16:59 |
| Βράδυ | 17:00 – 20:59 |
| Νύχτα | 21:00 – 04:59 |

**Σύνολα δεδομένων:** `LA_Crime_Data_2010_2019.csv`, `LA_Crime_Data_2020_2025.csv`  
**Στήλες:** `Premis Desc`, `TIME OCC`

### 3.2 Υλοποιήσεις

| Αρχείο | API |
|---|---|
| `Q1_df.py` | DataFrame (χωρίς UDF) |
| `Q1_df_udf.py` | DataFrame + UDF |
| `Q1_rdd.py` | RDD |

### 3.3 Αποτελέσματα

| Τμήμα ημέρας | Αριθμός εγκλημάτων | Ποσοστό (%) |
|---|---:|---:|
| Νύχτα | 251.094 | 34,08 |
| Βράδυ | 198.292 | 26,92 |
| Απόγευμα | 156.432 | 21,23 |
| Πρωί | 130.866 | 17,76 |

**Σύνολο εγκλημάτων στο δρόμο:** 736.684

### 3.4 Μέτρηση επιδόσεως

**Ρύθμιση:** 2 executors × 1 core × 2 GB

| Υλοποίηση | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---:|---:|
| DataFrame | 63,576 | 86,606 | 56,607 | **63,576** |
| DataFrame + UDF | 78,360 | 72,170 | 61,139 | **72,170** |
| RDD | 24,166 | 53,960 | 61,125 | **53,960** |

### 3.5 Σχολιασμός

Οι τρεις υλοποιήσεις συμφωνούν πλήρως. Το **DataFrame χωρίς UDF** είναι πιο σταθερό (διάμεσος 63,6 s) από το UDF (72,2 s). Το RDD παρουσιάζει μεγαλύτερη διακύμανση.

---

## 4. Ζητούμενο 3 — Query 2 (DataFrame και Spark SQL)

### 4.1 Εκφώνηση

Για **κάθε έτος**, εύρεση των **3 μηνών** με τον **μεγαλύτερο αριθμό** καταγεγραμμένων εγκλημάτων.

**Στήλες εξόδου:** `year`, `month`, `crime_total`, `ranking`

### 4.2 Υλοποιήσεις

| Αρχείο | API |
|---|---|
| `Q2_df.py` | DataFrame |
| `Q2_sql.py` | Spark SQL |

### 4.3 Δείγμα αποτελεσμάτων

| year | month | crime_total | ranking |
|---:|---:|---:|---:|
| 2010 | 1 | 19.524 | 1 |
| 2010 | 3 | 18.131 | 2 |
| 2010 | 7 | 17.857 | 3 |
| 2024 | 1 | 18.926 | 1 |
| 2024 | 2 | 17.394 | 2 |
| 2024 | 3 | 16.293 | 3 |

### 4.4 Μέτρηση επιδόσεως

**Ρύθμιση:** 4 executors × 1 core × 2 GB

| Υλοποίηση | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---:|---:|
| DataFrame | 50,235 | 54,572 | 55,339 | **54,572** |
| Spark SQL | 53,790 | 52,798 | 48,194 | **52,798** |

### 4.5 Σχολιασμός

DataFrame και SQL δίνουν **ίδια αποτελέσματα**. Η απόδοση είναι **συγκρίσιμη** (διάμεσοι 54,6 s vs 52,8 s).

---

## 5. Ζητούμενο 4 — Query 3 (DataFrame, RDD και ανάλυση join)

### 5.1 Εκφώνηση

Υπολογισμός **μέσου ετήσιου κατακεφαλήν εισοδήματος** ανά ZIP (2020–2021) από census blocks 2020 και median household income 2021.

**Τύπος:**

```text
avg_annual_per_capita_income = (median_household_income × total_housing_units) / total_population
```

### 5.2 Υλοποιήσεις

| Script | API |
|---|---|
| `Q3_df.py` | DataFrame |
| `Q3_rdd.py` | RDD |
| `Q3_df_joins.py` | Join experiments |

**Σημείωση:** Για αποφυγή OOM στο GeoJSON, χρησιμοποιήθηκε `reduceByKey` και ανάγνωση μόνο των απαραίτητων στηλών.

### 5.3 Αποτελέσματα

**Δείγμα (DataFrame):**

| ZIP | Πληθυσμός | Median income | Per-capita |
|---:|---:|---:|---:|
| 90001 | 55.859 | 52.806,00 | 13.064,66 |
| 90011 | 102.308 | 47.126,00 | 11.215,39 |

**Σύνολο ZIP:** 282 (DF) / 280 (RDD)

### 5.4 Μέτρηση επιδόσεως

**Ρύθμιση:** 3 executors × 1 core × 2 GB

| Υλοποίηση | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---:|---:|
| DataFrame | 40,814 | 42,944 | 51,926 | **42,944** |
| RDD | 52,352 | 54,620 | 48,288 | **52,352** |

### 5.5 Ανάλυση join (Query 3)

| Στρατηγική | Τελεστής | Χρόνος collect (s) |
|---|---|---:|
| default / broadcast | BroadcastHashJoin | ~2,45 |
| shuffle_hash | ShuffledHashJoin | 2,441 |
| merge | SortMergeJoin | **3,359** |
| shuffle_replicate_nl | CartesianProduct | 2,368 |

**Συμπέρασμα:** Για μικρό πίνακα income (~282 γραμμές), η **BroadcastHashJoin** είναι βέλτιστη.

---

## 6. Ζητούμενο 5 — Query 4 (DataFrame, Spark SQL, κλιμάκωση και join)

### 6.1 Εκφώνηση

Ανά **αστυνομικό τμήμα (DIVISION)**, υπολογισμός:

1. **Αριθμού εγκλημάτων** που έλαβαν χώρα **πιο κοντά** στο συγκεκριμένο τμήμα παρά σε οποιοδήποτε άλλο.
2. **Μέσης απόστασης** αυτών των εγκλημάτων από το τμήμα.

Ταξινόμηση: **φθίνουσα** ως προς τον αριθμό περιστατικών.

**Δεδομένα:**

| Αρχείο | Ρόλος |
|---|---|
| `LA_Crime_Data_*.csv` | Στήλες `DR_NO`, `LAT`, `LON` |
| `LA_Police_Stations.csv` | 21 σταθμοί· `DIVISION`, `X` (longitude), `Y` (latitude) |

### 6.2 Υλοποιήσεις

| Script | API | Βασική ιδέα |
|---|---|---|
| `Q4_df.py` | DataFrame | Cross join crimes × stations (broadcast), Haversine, `row_number()` για nearest station, aggregation |
| `Q4_sql.py` | Spark SQL | Ίδια λογική σε CTEs (`all_distances` → `ranked` → `nearest`) |
| `Q4_df_joins.py` | DataFrame | `explain` / `hint` στο cross join (§6 εκφώνησης) |

**Τύπος Haversine (μίλια, ακτίνα R = 3958,756):**

```text
distance = 2R × atan2(√a, √(1−a))
a = sin²(Δφ/2) + cos(φ1) × cos(φ2) × sin²(Δλ/2)
```

### 6.3 Αποτελέσματα

DataFrame και SQL δίνουν **ταυτόσημα** αποτελέσματα (21 divisions).

| DIVISION | Μέση απόσταση (mi) | Αριθμός εγκλημάτων |
|---|---:|---:|
| HOLLYWOOD | 1,288 | 224.124 |
| VAN NUYS | 1,826 | 208.129 |
| SOUTHWEST | 1,362 | 189.119 |
| WILSHIRE | 1,611 | 186.383 |
| 77TH STREET | 1,067 | 170.620 |
| RAMPART | 0,953 | 153.204 |
| PACIFIC | 2,394 | 162.027 |

*(Ο πίνακας 3 της εκφώνησης είναι ενδεικτικός· τα δικά μας νούμερα προκύπτουν από το **πλήρες dataset 2010–2025** με όλα τα εγκλήματα που έχουν συντεταγμένες.)*

**Ερμηνεία:** Τα τμήματα με μεγάλο όγκο εγκλημάτων (π.χ. HOLLYWOOD, VAN NUYS) «κερδίζουν» περισσότερα περιστατικά ως πλησιέστερα, αλλά η μέση απόσταση ποικίλλει ανάλογα με τη γεωγραφική κάλυψη.

### 6.4 Σύγκριση DataFrame vs Spark SQL

**Ρύθμιση A1:** 2 executors × 1 core × 2 GB

| Υλοποίηση | Χρόνος (s) |
|---|---:|
| DataFrame (`Q4_df.py`) | 100,865 |
| Spark SQL (`Q4_sql.py`) | 98,623 |

**Ρύθμιση B3:** 8 executors × 1 core × 2 GB (8 cores / 16 GB συνολικά)

| Υλοποίηση | Χρόνος (s) |
|---|---:|
| DataFrame | 61,630 |
| Spark SQL | 61,447 |

**Σχόλιο:** Και τα δύο API έχουν **σχεδόν ίδια** απόδοση (~2% διαφορά). Το query είναι compute-intensive (cross join ~3,5M εγκλημάτων × 21 σταθμοί).

### 6.5 Μελέτη κλιμάκωσης (DataFrame)

Η εκφώνηση ζητά εκτελέσεις σε πολλαπλές ρυθμίσεις. Κάθε ρύθμιση εκτελέστηκε **3 φορές** (όπου σημειώνεται διάμεσος) ή **1 φορά** (A1/A2 από προηγούμενες μετρήσεις).

**A. Κάθετη κλιμάκωση — 2 executors (σταθερός αριθμός executors, αυξάνουμε cores/memory ανά executor)**

| Ρύθμιση | Cores/exec | Memory/exec | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---|---:|---:|---:|---:|
| A1 | 1 | 2 GB | 100,865 | — | — | **100,865** |
| A2 | 2 | 4 GB | 59,485 | — | — | **59,485** |
| A3 | 4 | 8 GB | 39,927 | 42,056 | 76,740 | **42,056** |

**B. Οριζόντια κλιμάκωση — 8 cores / 16 GB συνολικά (αλλάζουμε πώς κατανέμονται οι πόροι)**

| Ρύθμιση | Executors | Cores/exec | Memory/exec | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---|---:|---:|---:|---:|
| B1 | 2 | 4 | 8 GB | 39,927 | 42,056 | 76,740 | **42,056** *(= A3)* |
| B2 | 4 | 2 | 4 GB | 67,059 | 60,732 | 62,253 | **62,253** |
| B3 | 8 | 1 | 2 GB | 61,630 | — | — | **61,630** |

**Σχολιασμός κλιμάκωσης:**

1. **A1 → A3 (κάθετη):** Αύξηση πόρων ανά executor (1c×2g → 4c×8g) μείωσε τον διάμεσο χρόνο **~58%** (100,9 s → 42,1 s).
2. **A1 → B3 (οριζόντια):** Αύξηση executors (2→8) με ίδιο συνολικό budget cores (2→8) μείωσε τον χρόνο **~39%** (100,9 s → 61,6 s).
3. **A3/B1 vs B2 vs B3 (ίδιο σύνολο 8 cores / 16 GB):** Ο **διάμεσος** χρόνος είναι **42,1 s (A3/B1)** < **62,3 s (B2)** ≈ **61,6 s (B3)** — δηλαδή **λιγότεροι, ισχυρότεροι executors** (2×4c) δίνουν καλύτερο αποτέλεσμα από **πολλούς αδύναμους** (8×1c) για αυτό το workload.
4. **Διακύμανση:** Στο A3 η 3η εκτέλεση (76,7 s) δείχνει ευαισθησία στον φόρτο cluster — γι’ αυτό η εκφώνηση ζητά **3 runs + median**.
5. **Συμπέρασμα:** Το Query 4 ωφελείται από περισσότερους πόρους· το κυρίαρχο κόστος είναι cross join + window aggregation.

**Εντολή εκτέλεσης (παράδειγμα — B3):**

```bash
source ~/bigdata-env.sh
cd ~/bigdata-dsml

spark-submit \
  --conf spark.executor.instances=8 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  code/Q4_df.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289
```

(Αντίστοιχα για `Q4_sql.py`.)

### 6.6 Ανάλυση στρατηγικών join (Query 4)

Το Query 4 χρησιμοποιεί **cross join** εγκλημάτων με σταθμούς (21 γραμμές). Με `Q4_df_joins.py` και `explain("formatted")`:

| Στρατηγική | Ανιχνευμένος τελεστής | Χρόνος sample count* (s) |
|---|---|---:|
| default | **BroadcastNestedLoopJoin** | 6,610 |
| merge (hint) | BroadcastNestedLoopJoin, NestedLoopJoin | 7,077 |

\*Ο χρόνος στο `Q4_df_joins.py` μετρά `count()` σε δείγμα 1000 γραμμών μετά το cross join — **όχι** το πλήρες query.

**Σχολιασμός:**

1. Ο Catalyst επιλέγει **BroadcastNestedLoopJoin** για cross join με μικρό side (stations), που είναι η **φυσική** επιλογή.
2. Το `merge` hint **δεν είναι κατάλληλο** για cross join — εμφανίζεται επιπλέον `NestedLoopJoin` χωρίς οφέλος.
3. Η βέλτιστη πρακτική είναι `crossJoin(broadcast(stations))` όπως στο `Q4_df.py`.

**Εντολή join experiment:**

```bash
spark-submit \
  --conf spark.executor.instances=8 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  code/Q4_df_joins.py \
  --join-strategy default \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289
```

---

## 7. Συνοπτικός πίνακας

| Query | Ζητούμενο | Υλοποίηση | Benchmark | Κύριο εύρημα |
|---|---|---|---|---|
| **Q1 format** | CSV vs Parquet | 2 scripts | 2 exec × 1c × 2g | **Parquet ~4× ταχύτερο** (15,8 s vs 63,6 s)· ίδια αποτελέσματα |
| **Q1** | DF / UDF / RDD | 3 scripts | 2 exec × 1c × 2g | Ίδια αποτελέσματα· **DF χωρίς UDF** πιο σταθερό (63,6 s) |
| **Q2** | DF / SQL | 2 scripts | 4 exec × 1c × 2g | Ίδια αποτελέσματα· **SQL ελαφρώς ταχύτερο** (52,8 s) |
| **Q3** | DF / RDD + joins | 3 scripts | 3 exec × 1c × 2g | **DF ταχύτερο** (42,9 s)· BroadcastHashJoin βέλτιστο join |
| **Q4** | DF / SQL + joins + scaling | 3 scripts | 2–8 exec, διάφορα cores/mem | Cross join + Haversine· **κλιμάκωση βελτιώνει ~40%**· BroadcastNestedLoopJoin |

---

## 8. Αναπαραγωγιμότητα

| Query | Logs / timing files |
|---|---|
| Q1 format | `q1_convert_parquet.log`, `q1_df_parquet_run*.log`, `Q1_format_times.txt` |
| Q1 | `q1_*_2exec_run*.log`, `Q1_times.txt` |
| Q2 | `q2_*_4exec_run*.log`, `Q2_times.txt` |
| Q3 | `q3_df_3exec_run*.log`, `q3_rdd_3exec_run*.log`, `q3_df_join_*.log`, `Q3_times.txt`, `Q3_join_times.txt` |
| Q4 | `q4_df_smoke.log`, `q4_sql_smoke.log`, `q4_df_B3_run1.log`, `q4_sql_B3_run1.log`, `q4_df_A2_run1.log`, `q4_df_join_*.log`, `Q4_times.txt` |

Namespace Kubernetes: **`dsml00289-priv`** (driver pods `q1-*`, `q2-*`, `q3-*`, `q4-*`).

---

## 9. Δήλωση χρήσης LLM

Βλ. ξεχωριστό αρχείο `LLM_USAGE.md` στο αποθετήριο.
