package com.sagar.hranalyticsv2;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.types.StructType;

import com.sagar.pojo.Employee;

import scala.Tuple2;

public class HRAnalyticsV2 {

	public static void main(String[] args) {

		System.out.println("Started");

		SparkSession sparkSession = SparkSession.builder().appName("HRAnalyticsV2").master("local[*]").getOrCreate();

		rDDTransformations(sparkSession);

		datasetTransformations(sparkSession);

		sparkSession.stop();

		System.out.println("Ended");

	}

	private static void datasetTransformations(SparkSession sparkSession) {
		// ✅ Define schema explicitly so Spark reads correct types
		StructType schema = new StructType().add("employee_id", "int").add("name", "string").add("department", "string")
				.add("salary", "int").add("joining_date", "string"); // We'll convert manually below

		// ✅ Read CSV
		Dataset<Row> df = sparkSession.read().option("header", true).schema(schema).csv("./data/input/*.csv");

		// ✅ Convert joining_date (string → proper date format)
		df = df.withColumn("joining_date", functions.to_date(functions.col("joining_date"), "M/d/yyyy"));

		// ✅ Create encoder for Employee POJO
		Encoder<Employee> empEncoder = Encoders.bean(Employee.class);

		// ✅ Convert DataFrame<Row> to Dataset<Employee>
		Dataset<Employee> empDataset = df.as(empEncoder);

		// ✅ Filter Example 1: Using Lambda
		Dataset<Employee> richEmployees1 = empDataset.filter((FilterFunction<Employee>) e -> e.getSalary() > 5000);

		richEmployees1.show();

		// ✅ Filter Example 2: Using SQL Expression
		Dataset<Employee> richEmployees2 = empDataset.filter("salary > 5000");

		richEmployees2.show();

		// ✅ Average Salary 1: Using API result DataFrame
		Dataset<Row> avgSalaryPerDepartment1 = empDataset.groupBy("department")
				.agg(functions.avg("salary").as("avg_salary"));

		avgSalaryPerDepartment1.show();

		// ✅ Average Salary 2: Using API result Dataset
		KeyValueGroupedDataset<String, Employee> groupByDept = empDataset
				.groupByKey((MapFunction<Employee, String>) Employee::getDepartment, Encoders.STRING());

		Dataset<Tuple2<String, Double>> avgSalaryPerDepartmentTyped = groupByDept
				.agg((TypedColumn<Employee, Double>) typed
						.avg((MapFunction<Employee, Double>) e -> e.getSalary().doubleValue()).name("avg_salary"));

		avgSalaryPerDepartmentTyped.show();

		// ✅ Average Salary 3: Using Spark SQL result Dataset
		empDataset.createOrReplaceTempView("employees");

		Dataset<Row> avgSalaryPerDepartment3 = empDataset.sparkSession()
				.sql("select department,AVG(salary) as avg_salary from employees group by department");

		avgSalaryPerDepartment3.show();

		// ✅ Average Salary 4: Using API result DataFrame starting with DataFrame
		Dataset<Row> empDataFrame = empDataset.toDF();

		empDataFrame.createOrReplaceTempView("employeesDF");

		Dataset<Row> avgSalaryPerDepartment4 = empDataFrame.sparkSession()
				.sql("select department,AVG(salary) as avg_salary from employeesDF group by department");

		avgSalaryPerDepartment4.show();

		// Practice code
		Encoder<Tuple2<Employee, String>> tupleEncoder = Encoders.tuple(empEncoder, Encoders.STRING());

		Dataset<Tuple2<Employee, String>> sampleMap = empDataset
				.map((MapFunction<Employee, Tuple2<Employee, String>>) e -> new Tuple2<>(e, e.getName()), tupleEncoder);
	}

	private static void rDDTransformations(SparkSession sparkSession) {
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<String> emps = javaSparkContext.textFile("./data/input/*.csv");
		// for (String e : emps.collect()) {
		// System.out.println(e);
		// }
		// System.out.println("-------------------");

		String header = emps.first();
		// System.out.println(header);

		emps = emps.filter(x -> !(x.equals(header)));

		JavaRDD<String[]> data = emps.map(x -> x.split(","));
		// for (String[] d : data.collect()) {
		// for (String id : d) {
		// System.out.print(id + ",");
		// }
		// System.out.println();
		// }
		// System.out.println("-------------------");

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		JavaRDD<Employee> employees = data.map(parts -> new Employee(Integer.parseInt(parts[0]), parts[1], parts[2],
				Integer.parseInt(parts[3]), new Date(sdf.parse(parts[4]).getTime())));

		// for (Employee d : employees.collect()) {
		// System.out.println(d.toString());
		// }
		// System.out.println("-------------------");

		System.out.println("Rich Employees (Salary > 5000)");

		// TODO
		// Rich Employees : having salary greater than 5000
		JavaRDD<Employee> richEmployees = employees.filter(e -> e.getSalary() > 5000);

		for (Employee d : richEmployees.collect()) {
			System.out.println(d.toString());
		}
		System.out.println("-------------------");

		// TODO
		// Avg Salary : Average salary of employees per department
		JavaPairRDD<String, Integer> deptSalaryPairs = employees
				.mapToPair(e -> new Tuple2<>(e.getDepartment(), e.getSalary()));

		// for (Tuple2<String, Integer> d : deptSalaryPairs.collect()) {
		// System.out.println(d.toString());
		// }
		// System.out.println("-------------------");

		JavaPairRDD<String, Tuple2<Integer, Integer>> deptSumCount = deptSalaryPairs
				.mapValues(sal -> new Tuple2<>(sal, 1));// .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 +
														// b._2));

		// for (Tuple2<String, Tuple2<Integer, Integer>> d : deptSumCount.collect()) {
		// System.out.println(d.toString());
		// }
		// System.out.println("-------------------");

		JavaPairRDD<String, Tuple2<Integer, Integer>> deptSumCountR = deptSumCount
				.reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

		// for (Tuple2<String, Tuple2<Integer, Integer>> d : deptSumCountR.collect()) {
		// System.out.println(d.toString());
		// }
		// System.out.println("-------------------");

		JavaPairRDD<String, Double> deptSumCountAvg = deptSumCountR
				.mapValues(sc -> Double.valueOf(sc._1) / Double.valueOf(sc._2));

		System.out.println("Average Salary per Department");

		for (Tuple2<String, Double> d : deptSumCountAvg.collect()) {
			System.out.println(d.toString());
		}
		System.out.println("-------------------");

		// TODO
		// Top Earners : Top 2 earners per department
		JavaPairRDD<String, Tuple2<Employee, Integer>> empSalaryPairs = employees
				.mapToPair(e -> new Tuple2<>(e.getDepartment(), new Tuple2<>(e, e.getSalary())));

		// for (Tuple2<String, Tuple2<Employee, Integer>> d : empSalaryPairs.collect())
		// {
		// System.out.println(d.toString());
		// }
		// System.out.println("-------------------");

		JavaPairRDD<String, Iterable<Tuple2<Employee, Integer>>> empSalaryPairsGroup = empSalaryPairs.groupByKey();

		JavaPairRDD<String, Iterable<Tuple2<Employee, Integer>>> top2perDept = empSalaryPairsGroup.mapValues(itr -> {
			List<Tuple2<Employee, Integer>> list = new ArrayList<>();
			itr.forEach(list::add);
			list.sort((a, b) -> b._2.compareTo(a._2));
			// return list.subList(0, Math.min(2, list.size()));
			return new ArrayList<>(list.subList(0, Math.min(2, list.size())));

		});

		System.out.println("Top 2 Earners per Department");

		for (Tuple2<String, Iterable<Tuple2<Employee, Integer>>> a : top2perDept.collect()) {
			System.out.println("Department : " + a._1);
			for (Tuple2<Employee, Integer> b : a._2) {
				System.out.println(b._1.getName() + "," + b._2);
			}
			System.out.println("-------------------");
		}

		JavaPairRDD<String, Tuple2<String, Integer>> flat = top2perDept.flatMapToPair(dept -> {
			List<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<>();
			for (Tuple2<Employee, Integer> e : dept._2) {
				list.add(new Tuple2<>(dept._1, new Tuple2<>(e._1.getName(), e._2)));
			}
			return list.iterator();
		});

		System.out.println("Top 2 Earners per Department (Flattened)");

		for (Tuple2<String, Tuple2<String, Integer>> d : flat.collect()) {
			System.out.println(d);
		}

		System.out.println("-------------------");
	}
}
