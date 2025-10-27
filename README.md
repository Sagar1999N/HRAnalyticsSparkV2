# 💼 HR Analytics using Apache Spark (Java RDD)

A practical **Apache Spark RDD project in Java** that performs HR analytics tasks such as filtering high-salary employees, calculating department-wise average salary, and identifying the top 2 earners per department.

This project is part of my **Data Engineering Learning Series** — focused on mastering Spark RDDs, transformations, actions, and real-world data manipulation using Java.

---

## 🚀 Project Overview

This Spark application reads employee data from CSV files and performs the following analytics:

1. **Filter rich employees** — Employees with salary > 5000  
2. **Average salary by department** — Using `mapToPair`, `reduceByKey`, and `mapValues`  
3. **Top 2 earners per department** — Using `groupByKey`, sorting, and sublisting logic  

---

## 🧠 Concepts Covered

- Spark RDD creation using `textFile()`  
- Transformations: `map`, `filter`, `mapToPair`, `reduceByKey`, `groupByKey`, `mapValues`, `flatMapToPair`  
- Actions: `collect()`, `first()`  
- Using `Tuple2` for key-value pair manipulations  
- Creating and mapping custom POJOs (`Employee`)  
- Parsing CSV data and handling headers  
- Sorting and sublisting collections in Spark  

---

## 🏗️ Project Structure
```
HrAnalyticsSparkV2/
│
├── src/main/java/com/sagar/hranalyticsv2/
│ └── HRAnalyticsV2.java # Main Spark Application
│
├── src/main/java/com/sagar/pojo/
│ └── Employee.java # Employee POJO (id, name, department, salary, doj)
│
├── data/input/
│ └── employees.csv # Sample input dataset
│
├── pom.xml # Maven dependencies (Spark, Scala, etc.)
└── README.md # Project documentation
```
---

## 🧩 Sample Input (employees.csv)

```csv
id,name,department,salary,doj
1,John,IT,7000,2021-01-01
2,Alice,HR,5500,2020-06-12
3,Bob,IT,6000,2021-07-23
4,Eve,Finance,4500,2019-03-05
5,Charlie,HR,8000,2022-02-14
6,David,Finance,9000,2020-08-30
7,Raj,IT,10000,2023-04-10
```
---
## ⚙️ How to Run
### Clone the Repository

bash
```
git clone https://github.com/<your-username>/HrAnalyticsSparkV2.git
cd HrAnalyticsSparkV2
Open in IDE (STS / Eclipse)
Ensure Maven and JDK 8+ are configured properly.
```
### Run the Main Class
```
HRAnalyticsV2.java
```
### Expected Output (Examples)

```
************************************************************
**** APPLICATION STARTED ****
************************************************************


==================== RDD TRANSFORMATIONS STARTED ====================


---------- RICH EMPLOYEES (SALARY > 5000) ----------

Employee [employee_id=1, name=John, department=IT, salary=7000, joining_date=2021-01-01]
Employee [employee_id=2, name=Alice, department=HR, salary=5500, joining_date=2020-06-12]
Employee [employee_id=3, name=Bob, department=IT, salary=6000, joining_date=2021-07-23]
Employee [employee_id=5, name=Charlie, department=HR, salary=8000, joining_date=2022-02-14]
Employee [employee_id=6, name=David, department=Finance, salary=9000, joining_date=2020-08-30]
Employee [employee_id=7, name=Raj, department=IT, salary=10000, joining_date=2023-04-10]

------------------------------------------------------------


---------- AVERAGE SALARY PER DEPARTMENT ----------

(HR,6750.0)
(Finance,6750.0)
(IT,7666.666666666667)

------------------------------------------------------------


---------- TOP 2 EARNERS PER DEPARTMENT ----------

Department : HR
Charlie,8000
Alice,5500
-------------------
Department : Finance
David,9000
Eve,4500
-------------------
Department : IT
Raj,10000
John,7000
-------------------

------------------------------------------------------------


---------- TOP 2 EARNERS (FLATTENED VIEW) ----------

(HR,(Charlie,8000))
(HR,(Alice,5500))
(Finance,(David,9000))
(Finance,(Eve,4500))
(IT,(Raj,10000))
(IT,(John,7000))

==================== RDD TRANSFORMATIONS ENDED ====================


------------------------------------------------------------


==================== DATASET TRANSFORMATIONS STARTED ====================


---------- FILTER EXAMPLES ----------

+-----------+-------+----------+------+------------+
|employee_id|   name|department|salary|joining_date|
+-----------+-------+----------+------+------------+
|          1|   John|        IT|  7000|        NULL|
|          2|  Alice|        HR|  5500|        NULL|
|          3|    Bob|        IT|  6000|        NULL|
|          5|Charlie|        HR|  8000|        NULL|
|          6|  David|   Finance|  9000|        NULL|
|          7|    Raj|        IT| 10000|        NULL|
+-----------+-------+----------+------+------------+


------------------------------------------------------------

+-----------+-------+----------+------+------------+
|employee_id|   name|department|salary|joining_date|
+-----------+-------+----------+------+------------+
|          1|   John|        IT|  7000|        NULL|
|          2|  Alice|        HR|  5500|        NULL|
|          3|    Bob|        IT|  6000|        NULL|
|          5|Charlie|        HR|  8000|        NULL|
|          6|  David|   Finance|  9000|        NULL|
|          7|    Raj|        IT| 10000|        NULL|
+-----------+-------+----------+------+------------+


---------- AVERAGE SALARY EXAMPLES ----------

+----------+-----------------+
|department|       avg_salary|
+----------+-----------------+
|        HR|           6750.0|
|   Finance|           6750.0|
|        IT|7666.666666666667|
+----------+-----------------+


------------------------------------------------------------

+-------+-----------------+
|    key|       avg_salary|
+-------+-----------------+
|     HR|           6750.0|
|Finance|           6750.0|
|     IT|7666.666666666667|
+-------+-----------------+


------------------------------------------------------------

+----------+-----------------+
|department|       avg_salary|
+----------+-----------------+
|        HR|           6750.0|
|   Finance|           6750.0|
|        IT|7666.666666666667|
+----------+-----------------+


------------------------------------------------------------

+----------+-----------------+
|department|       avg_salary|
+----------+-----------------+
|        HR|           6750.0|
|   Finance|           6750.0|
|        IT|7666.666666666667|
+----------+-----------------+


---------- MAP EXAMPLE (TUPLE CREATION) ----------


==================== DATASET TRANSFORMATIONS ENDED ====================


************************************************************
**** APPLICATION ENDED ****
************************************************************
```

---

## 📦 Dependencies
- Java 8+
- Apache Spark 3.x
- Maven
- Scala Library (auto-included via Spark dependencies)

---

## 🧑‍💻 Author
Sagar
Aspiring Data Engineer | Building bulletproof understanding of Spark & Big Data.
🔗 [LinkedIn](https://www.linkedin.com/in/sagar-nalawade-a53bb3231/) | 🐙 [GitHub](https://github.com/Sagar1999N)

---

### 💡 This project is part of my ongoing Data Engineering mastery series — building from RDDs to real-world Spark pipelines.
