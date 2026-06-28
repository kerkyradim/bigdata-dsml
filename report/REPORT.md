# Εξαμηνιαία Εργασία DSML 2026 — Αναφορά Queries 1–4

**Ονοματεπώνυμο:** Κερκύρα Δημησιάνου  
**Α.Μ.:** dsml00289  
**Μάθημα:** Διαχείριση Δεδομένων Μεγάλης Κλίμακας  
**Περιβάλλον:** Apache Spark 3.5.8, Kubernetes (lab cluster), HDFS  
**Αποθετήριο κώδικα:** https://github.com/kerkyradim/bigdata-dsml

---

## 1. Περιβάλλον εκτέλεσης

Οι υλοποιήσεις εκτελέστηκαν από WSL με `spark-submit` σε **cluster mode** στο Kubernetes cluster του εργαστηρίου. Τα δεδομένα εισόδου διαβάστηκαν από το HDFS (`/data/`), ενώ τα αποτελέσματα και τα logs αποθηκεύτηκαν κάτω από `/user/dsml00289/`.

Ο χρόνος εκτέλεσης μετρήθηκε **μέσα στον κώδικα Python** με `time.perf_counter()` και τυπώνεται ως `QUERY_ELAPSED_SECONDS`. Τα αποτελέσματα των μετρήσεων καταγράφηκαν στα αρχεία:

- `results/Q1_times.txt`
- `results/Q1_format_times.txt`
- `results/Q2_times.txt`
- `results/Q3_times.txt`
- `results/Q3_join_times.txt`


Για τα Queries 1–3, κάθε ρύθμιση εκτελέστηκε **3 φορές** και αναφέρεται ο **διάμεσος (median)** χρόνος. 

**Παράδειγμα εκτέλεσης:**

```bash
source ~/bigdata-env.sh
cd ~/dsml00289-bigdata

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



## 2. Ζητούμενο 2 — Query 1 (DataFrame, DataFrame+UDF, RDD)

### 2.1 Query

Ταξινόμηση των τμημάτων της ημέρας σε φθίνουσα σειρά ως προς το ποσοστό εγκλημάτων που έλαβαν χώρα στο (`STREET`) επί του συνόλου των εγκλημάτων στο δρόμο.

| Τμήμα ημέρας | Ώρες |
|---|---|
| Πρωί | 05:00 – 11:59 |
| Απόγευμα | 12:00 – 16:59 |
| Βράδυ | 17:00 – 20:59 |
| Νύχτα | 21:00 – 04:59 |

**Σύνολα δεδομένων:** `LA_Crime_Data_2010_2019.csv`, `LA_Crime_Data_2020_2025.csv`  
**Στήλες:** `Premis Desc`, `TIME OCC`

### 2.2 Υλοποιήσεις

| Αρχείο | API |
|---|---|
| `Q1_df.py` | DataFrame (χωρίς UDF) |
| `Q1_df_udf.py` | DataFrame + UDF |
| `Q1_rdd.py` | RDD |

### 2.3 Αποτελέσματα

| Τμήμα ημέρας | Αριθμός εγκλημάτων | Ποσοστό (%) |
|---|---:|---:|
| Νύχτα | 251.094 | 34,08 |
| Βράδυ | 198.292 | 26,92 |
| Απόγευμα | 156.432 | 21,23 |
| Πρωί | 130.866 | 17,76 |

**Σύνολο εγκλημάτων στο "STREET":** 736.684

### 2.4 Μέτρηση επιδόσεων

**Ρύθμιση:** 2 executors × 1 core × 2 GB

| Υλοποίηση | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---:|---:|
| DataFrame | 63,576 | 86,606 | 56,607 | **63,576** |
| DataFrame + UDF | 78,360 | 72,170 | 61,139 | **72,170** |
| RDD | 24,166 | 53,960 | 61,125 | **53,960** |

### 2.5 Σχολιασμός

Οι τρεις υλοποιήσεις συμφωνούν πλήρως. Το **DataFrame χωρίς UDF** είναι πιο σταθερό (διάμεσος 63,6 s) από το UDF (72,2 s). Αυτό οφείλεται στον εσωτερικό μηχανισμό βελτιστοποίησης στο Spark. Όταν χρησιμοποιούμε native συναρτήσεις του Spark, ο Catalyst Optimizer δημιουργεί ένα ενιαίο, βελτιστοποιημένο πλάνο εκτέλεσης. Αντίθετα, με την εισαγωγή μιας Python UDF, ο Catalyst αναγκάζεται να αντιμετωπίσει τη συνάρτηση ως «μαύρο κουτί», ακυρώνοντας τις αυτοματοποιημένες βελτιστοποιήσεις.  Το RDD παρουσιάζει μεγαλύτερη διακύμανση. Αυτό συμβαίνει επειδή τα RDDs λειτουργούν σε χαμηλό επίπεδο (low-level). Είναι, επομένως, εξαιρετικά ευάλωτα στις τρέχουσες συνθήκες του cluster (π.χ. αυξημένος φόρτος από άλλους χρήστες).


## 2. CSV vs Parquet ( for Query 1)

### 2.1 Σκοπός

Μετατροπή των CSV αρχείων εγκλημάτων σε **Parquet** και σύγκριση χρόνων εκτέλεσης του **Query 1** ( στο DataFrame, ίδια λογική με `Q1_df.py`) για τις δύο μορφές.

### 2.2 Γιατί Parquet?

Το CSV είναι μορφή κειμένου γραμμή-προς-γραμμή:  κάθε εκτέλεση απαιτεί parsing strings, inferencing τύπων και ανάγνωση όλων των στηλών, ακόμη κι αν το query χρησιμοποιεί λίγες.

Το Parquet είναι columnar format: τα δεδομένα αποθηκεύονται ανά στήλη, επιτρέποντας column pruning (διαβάζονται μόνο `Premis Desc` και `TIME OCC`) και predicate pushdown. Στο Spark, η ανάγνωση Parquet αποφεύγει το βαρύ CSV parsing και μειώνει σημαντικά τον όγκο I/O από το HDFS.

### 2.3 Μετατροπή CSV → Parquet

**Script:** `Q1_convert_parquet.py` 

```bash
source ~/bigdata-env.sh
cd ~/dsml00289-bigdata

spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.kubernetes.submission.waitAppCompletion=true \
  code/Q1_convert_parquet.py \
  --output hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/dsml00289/LA_Crime_Data_parquet
```

**Αποτέλεσμα μετατροπής:**  `ROW_COUNT=3.138.128` γραμμές → `hdfs://.../user/dsml00289/LA_Crime_Data_parquet`

### 2.4 Σύγκριση Query 1 (DataFrame)

**Ρύθμιση benchmark:** 2 executors × 1 core × 2 GB 

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

#### Αποτελέσματα 

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

**Επιτάχυνση:** ~**4.0×** (63,6 s → 15,8 s αν συγκρίνουμε με βάση τον μέσο όρο).

#### Όγκος στο HDFS

| Μορφή | Μέγεθος (hdfs dfs -du -s -h) |
|---|---:|
| CSV (2 αρχεία πηγής) | **896,2 MB** (608,6 + 287,6 MB) |
| Parquet | **127,8 MB** |

Παρατηρούμε ότι η συγκεκριμένη μετατροπή επέφερε μείωση του αποθηκευτικού όγκου κατά ~7×, αποσυμφορώντας σημαντικά το δίκτυο (network I/O) του cluster κατά τη φάση ανάγνωσης των δεδομένων.

### 2.5 Σχολιασμός

Το Parquet βελτιώνει δραματικά τον χρόνο του Q1, επειδή το query αγγίζει μόνο δύο στήλες από εκατομμύρια εγγραφές. Ο Catalyst Optimizer περιόρισε την ανάγνωση από το HDFS αποκλειστικά στα blocks των στηλών ```Premis Desc και TIME OCC```, αγνοώντας το υπόλοιπο 90% του όγκου του dataset. Το CSV αναγκάζει ανάγνωση και parsing ολόκληρων γραμμών. 
Επιπλέον, η μείωση του αποθηκευτικού χώρου στο HDFS κατά ~85% (από 896,2 MB σε 127,8 MB) λόγω της συμπίεσης Snappy, αποδεικνύει ότι το Parquet μειώνει δραστικά το disk serialization overhead. Παρά το γεγονός ότι η μετατροπή (CSV $\rightarrow$ Parquet) έχει ένα αρχικό υπολογιστικό κόστος (one-time cost), αυτό αντισταθμίζεται άμεσα σε περιβάλλοντα παραγωγής όπου τα ίδια δεδομένα υφίστανται συνεχή και επαναλαμβανόμενα ερωτήματα ανάλυσης.

Logs/timing files: `results/Q1_format_times.txt`, `results/logs/q1_df_parquet_run*.log`

---

## 3. Ζητούμενο 3 — Query 2 (DataFrame και Spark SQL)

### 3.1 Query

Για κάθε έτος, εύρεση των 3 μηνών με τον μεγαλύτερο αριθμό καταγεγραμμένων εγκλημάτων.

### 3.2 Υλοποιήσεις

| Αρχείο | API |
|---|---|
| `Q2_df.py` | DataFrame |
| `Q2_sql.py` | Spark SQL |

### 3.3 Δείγμα αποτελεσμάτων

| year | month | crime_total | ranking |
|---:|---:|---:|---:|
| 2010 | 1 | 19.524 | 1 |
| 2010 | 3 | 18.131 | 2 |
| 2010 | 7 | 17.857 | 3 |
| 2024 | 1 | 18.926 | 1 |
| 2024 | 2 | 17.394 | 2 |
| 2024 | 3 | 16.293 | 3 |

### 3.4 Μέτρηση επιδόσεως

**Ρύθμιση:** 4 executors × 1 core × 2 GB

| Υλοποίηση | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---:|---:|
| DataFrame | 50,235 | 54,572 | 55,339 | **54,572** |
| Spark SQL | 53,790 | 52,798 | 48,194 | **52,798** |

### 3.5 Σχολιασμός

Όπως προκύπτει από τις μετρήσεις, το DataFrame API και το Spark SQL API δίνουν ταυτόσημα αποτελέσματα και σχεδόν ολόιδιους χρόνους εκτέλεσης (54,6 s έναντι 52,8 s). Αυτή η μικρή διαφορά των ~1,8 δευτερολέπτων είναι αμελητέα και οφείλεται αποκλειστικά σε στιγμιαίες αυξομειώσεις στον φόρτο του cluster. Όταν υποβάλλεται ένα query —είτε ως string SQL είτε ως αλυσίδα μεθόδων DataFrame— ο Catalyst Optimizer αναλαμβάνει να το αναλύσει και να το μεταφράσει στο ίδιο ακριβώς βελτιστοποιημένο Φυσικό Πλάνο (Physical Plan).

---

## 4. Ζητούμενο 4 — Query 3 (DataFrame, RDD και ανάλυση join)

### 4.1 Query

Υπολογισμός μέσου ετήσιου κατακεφαλήν εισοδήματος ανά ZIP  code (2020–2021) από census blocks 2020 και median household income 2021.

**Τύπος:**

```text
avg_annual_per_capita_income = (median_household_income × total_housing_units) / total_population
```

### 4.2 Υλοποιήσεις

| Script | API |
|---|---|
| `Q3_df.py` | DataFrame |
| `Q3_rdd.py` | RDD |
| `Q3_df_joins.py` | Join experiments |

**Σημείωση:** Για αποφυγή OOM (out of memory) στο GeoJSON, χρησιμοποιήθηκε `reduceByKey` και ανάγνωση μόνο των απαραίτητων στηλών. Επειδή το αρχείο GeoJSON περιέχει βαριά γεωγραφικά δεδομένα, υπήρχε πιθανότατα κίνδυνος Out-Of-Memory (OOM) στους executors των 2 GB. Για να αποφευχθεί αυτό, απομονώθηκαν αμέσως μόνο οι απαραίτητες αριθμητικές στήλες και χρησιμοποιήθηκε η reduceByKey στο RDD, η οποία εκτελεί map-side συνδυασμό (combiner) των δεδομένων ανά ZIP Code, μειώνοντας δραστικά τον όγκο των δεδομένων που μεταφέρονται στη μνήμη κατά το shuffle.

### 4.3 Αποτελέσματα

**Δείγμα (DataFrame):**

| ZIP | Πληθυσμός | Median income | Per-capita |
|---:|---:|---:|---:|
| 90001 | 55.859 | 52.806,00 | 13.064,66 |
| 90011 | 102.308 | 47.126,00 | 11.215,39 |

**Σύνολο ZIP codes:** 282 (DF) / 280 (RDD)

### 4.4 Μέτρηση επιδόσεων

**Ρύθμιση:** 3 executors × 1 core × 2 GB

| Υλοποίηση | Εκτ. 1 | Εκτ. 2 | Εκτ. 3 | **Διάμεσος** |
|---|---:|---:|---:|---:|
| DataFrame | 40,814 | 42,944 | 51,926 | **42,944** |
| RDD | 52,352 | 54,620 | 48,288 | **52,352** |

### 4.5 Ανάλυση join (Query 3)

| Στρατηγική | Τελεστής | Χρόνος collect (s) |
|---|---|---:|
| default / broadcast | BroadcastHashJoin | ~2,45 |
| shuffle_hash | ShuffledHashJoin | 2,441 |
| merge | SortMergeJoin | 3,359 |
| shuffle_replicate_nl | CartesianProduct | 2,368 |

**Συμπέρασμα:** 
1. ***Default επιλογή Optimizer***: Παρατηρούμε ότι στην επιλογή default, ο Catalyst Optimizer επιλέγει ορθά το BroadcastHashJoin. Καθώς το αρχείο εισοδήματος (LA_income_2021.csv) είναι εξαιρετικά μικρό, το Spark επιλέγει να το αποστείλει αυτούσιο (broadcast) σε όλους τους executors. Με αυτόν τον τρόπο, αποφεύγεται πλήρως το ακριβό network shuffle. 

2. ***Καθυστέρηση στο SortMergeJoin (merge)***: Η στρατηγική merge ανάγκασε το Spark να εκτελέσει SortMergeJoin, προκαλώντας τη μεγαλύτερη καθυστέρηση (3.359 s, επιβάρυνση ~37%). Αυτό είναι απόλυτα λογικό, καθώς η στρατηγική αυτή επιβάλλει shuffle και σταθερή ταξινόμηση (sorting) των κλειδιών πριν από την ένωση, μια διαδικασία  ασύμφορη για δεδομένα μικρής κλίμακας.

3. ***Καταλληλότητα Στρατηγικής:*** Για το συγκεκριμένο Query, η καταλληλότερη στρατηγική είναι ξεκάθαρα η BroadcastHashJoin (είτε μέσω default είτε μέσω ρητού broadcast hint), καθώς ελαχιστοποιεί τις μεταφορές δεδομένων στο δίκτυο και εκμεταλλεύεται πλήρως τη διαθέσιμη μνήμη RAM των executors.

---



## 6. Συνοπτικός πίνακας

| Query | Ζητούμενο | Υλοποίηση | Benchmark | Κύριο εύρημα |
|---|---|---|---|---|
| **Q1** | DF / UDF / RDD | 3 scripts | 2 exec × 1c × 2g | Ίδια αποτελέσματα· **DF χωρίς UDF** πιο σταθερό (63,6 s) |
| **Q2** | DF / SQL | 2 scripts | 4 exec × 1c × 2g | Ίδια αποτελέσματα· **SQL ελαφρώς ταχύτερο** (52,8 s) |
| **Q3** | DF / RDD + joins | 3 scripts | 3 exec × 1c × 2g | **DF ταχύτερο** (42,9 s)· BroadcastHashJoin βέλτιστο join |


---

## 7. Αποτελέσματα ερωτημάτων

| Query | Logs / timing files |
|---|---|
| Q1 | `results/logs/q1_*_2exec_run*.log`, `Q1_times.txt`, `Q1_format_times.txt` |
| Q2 | `results/logs/q2_*_4exec_run*.log`, `Q2_times.txt` |
| Q3 | `results/logs/q3_df_3exec_run*.log`, `results/logs/q3_rdd_3exec_run*.log`, `results/logs/q3_df_join_*.log`, `Q3_times.txt`, `Q3_join_times.txt` |

Namespace Kubernetes: **`dsml00289-priv`** (driver pods `q1-*`, `q2-*`, `q3-*`).

---

## 8. Δήλωση χρήσης LLM

Βλ. ξεχωριστό αρχείο [`LLM_USAGE.md`](../LLM_USAGE.md) στο αποθετήριο.
