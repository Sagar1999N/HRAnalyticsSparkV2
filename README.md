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

Rich Employees (salary > 5000)
```John, IT, 7000
Alice, HR, 5500
```
Average Salary per Department
```
(IT, 7666.67)
(HR, 6750.0)
(Finance, 6750.0)
```
Top 2 Earners per Department
```
Department : IT
Raj, 10000
John, 7000
-------------------
Department : HR
Charlie, 8000
Alice, 5500
-------------------
Department : Finance
David, 9000
Eve, 4500
-------------------
```
## 📦 Dependencies
- Java 8+
- Apache Spark 3.x
- Maven
- Scala Library (auto-included via Spark dependencies)

## 🧑‍💻 Author
Sagar
Aspiring Data Engineer | Building bulletproof understanding of Spark & Big Data.
🔗 [LinkedIn](https://www.linkedin.com/in/sagar-nalawade-a53bb3231/) | 🐙 [GitHub](https://github.com/Sagar1999N)

## 🏁 Next Steps
 ✅ Add DataFrame/Dataset version of same logic
 
 ✅ Extend project to handle missing values & data validation
 
 ✅ Visualize average salaries using Spark SQL + Pandas

### 💡 This project is part of my ongoing Data Engineering mastery series — building from RDDs to real-world Spark pipelines.
