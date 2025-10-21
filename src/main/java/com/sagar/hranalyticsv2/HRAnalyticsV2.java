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

		printBanner("APPLICATION STARTED");

		SparkSession sparkSession = SparkSession.builder().appName("HRAnalyticsV2").master("local[*]").getOrCreate();

		rDDTransformations(sparkSession);

		printDivider();

		datasetTransformations(sparkSession);

		sparkSession.stop();

		printBanner("APPLICATION ENDED");
	}

	private static void datasetTransformations(SparkSession sparkSession) {

		printSection("DATASET TRANSFORMATIONS STARTED");

		StructType schema = new StructType().add("employee_id", "int").add("name", "string").add("department", "string")
				.add("salary", "int").add("joining_date", "string");

		Dataset<Row> df = sparkSession.read().option("header", true).schema(schema).csv("./data/input/*.csv");

		df = df.withColumn("joining_date", functions.to_date(functions.col("joining_date"), "M/d/yyyy"));

		Encoder<Employee> empEncoder = Encoders.bean(Employee.class);
		Dataset<Employee> empDataset = df.as(empEncoder);

		printSubSection("FILTER EXAMPLES");
		Dataset<Employee> richEmployees1 = empDataset.filter((FilterFunction<Employee>) e -> e.getSalary() > 5000);
		richEmployees1.show();

		printDivider();

		Dataset<Employee> richEmployees2 = empDataset.filter("salary > 5000");
		richEmployees2.show();

		printSubSection("AVERAGE SALARY EXAMPLES");

		Dataset<Row> avgSalaryPerDepartment1 = empDataset.groupBy("department")
				.agg(functions.avg("salary").as("avg_salary"));
		avgSalaryPerDepartment1.show();

		printDivider();

		KeyValueGroupedDataset<String, Employee> groupByDept = empDataset
				.groupByKey((MapFunction<Employee, String>) Employee::getDepartment, Encoders.STRING());
		Dataset<Tuple2<String, Double>> avgSalaryPerDepartmentTyped = groupByDept
				.agg((TypedColumn<Employee, Double>) typed
						.avg((MapFunction<Employee, Double>) e -> e.getSalary().doubleValue()).name("avg_salary"));
		avgSalaryPerDepartmentTyped.show();

		printDivider();

		empDataset.createOrReplaceTempView("employees");
		Dataset<Row> avgSalaryPerDepartment3 = empDataset.sparkSession()
				.sql("select department,AVG(salary) as avg_salary from employees group by department");
		avgSalaryPerDepartment3.show();

		printDivider();

		Dataset<Row> empDataFrame = empDataset.toDF();
		empDataFrame.createOrReplaceTempView("employeesDF");
		Dataset<Row> avgSalaryPerDepartment4 = empDataFrame.sparkSession()
				.sql("select department,AVG(salary) as avg_salary from employeesDF group by department");
		avgSalaryPerDepartment4.show();

		printSubSection("MAP EXAMPLE (TUPLE CREATION)");
		Encoder<Tuple2<Employee, String>> tupleEncoder = Encoders.tuple(empEncoder, Encoders.STRING());
		Dataset<Tuple2<Employee, String>> sampleMap = empDataset
				.map((MapFunction<Employee, Tuple2<Employee, String>>) e -> new Tuple2<>(e, e.getName()), tupleEncoder);

		printSection("DATASET TRANSFORMATIONS ENDED");
	}

	private static void rDDTransformations(SparkSession sparkSession) {

		printSection("RDD TRANSFORMATIONS STARTED");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
		JavaRDD<String> emps = javaSparkContext.textFile("./data/input/*.csv");

		String header = emps.first();
		emps = emps.filter(x -> !(x.equals(header)));

		JavaRDD<String[]> data = emps.map(x -> x.split(","));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		JavaRDD<Employee> employees = data.map(parts -> new Employee(Integer.parseInt(parts[0]), parts[1], parts[2],
				Integer.parseInt(parts[3]), new Date(sdf.parse(parts[4]).getTime())));

		printSubSection("RICH EMPLOYEES (SALARY > 5000)");
		JavaRDD<Employee> richEmployees = employees.filter(e -> e.getSalary() > 5000);
		for (Employee d : richEmployees.collect()) {
			System.out.println(d.toString());
		}

		printDivider();

		printSubSection("AVERAGE SALARY PER DEPARTMENT");
		JavaPairRDD<String, Integer> deptSalaryPairs = employees
				.mapToPair(e -> new Tuple2<>(e.getDepartment(), e.getSalary()));
		JavaPairRDD<String, Tuple2<Integer, Integer>> deptSumCount = deptSalaryPairs
				.mapValues(sal -> new Tuple2<>(sal, 1));
		JavaPairRDD<String, Tuple2<Integer, Integer>> deptSumCountR = deptSumCount
				.reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));
		JavaPairRDD<String, Double> deptSumCountAvg = deptSumCountR
				.mapValues(sc -> Double.valueOf(sc._1) / Double.valueOf(sc._2));

		for (Tuple2<String, Double> d : deptSumCountAvg.collect()) {
			System.out.println(d.toString());
		}

		printDivider();

		printSubSection("TOP 2 EARNERS PER DEPARTMENT");
		JavaPairRDD<String, Tuple2<Employee, Integer>> empSalaryPairs = employees
				.mapToPair(e -> new Tuple2<>(e.getDepartment(), new Tuple2<>(e, e.getSalary())));
		JavaPairRDD<String, Iterable<Tuple2<Employee, Integer>>> empSalaryPairsGroup = empSalaryPairs.groupByKey();
		JavaPairRDD<String, Iterable<Tuple2<Employee, Integer>>> top2perDept = empSalaryPairsGroup.mapValues(itr -> {
			List<Tuple2<Employee, Integer>> list = new ArrayList<>();
			itr.forEach(list::add);
			list.sort((a, b) -> b._2.compareTo(a._2));
			return new ArrayList<>(list.subList(0, Math.min(2, list.size())));
		});

		for (Tuple2<String, Iterable<Tuple2<Employee, Integer>>> a : top2perDept.collect()) {
			System.out.println("Department : " + a._1);
			for (Tuple2<Employee, Integer> b : a._2) {
				System.out.println(b._1.getName() + "," + b._2);
			}
			System.out.println("-------------------");
		}

		printDivider();

		printSubSection("TOP 2 EARNERS (FLATTENED VIEW)");
		JavaPairRDD<String, Tuple2<String, Integer>> flat = top2perDept.flatMapToPair(dept -> {
			List<Tuple2<String, Tuple2<String, Integer>>> list = new ArrayList<>();
			for (Tuple2<Employee, Integer> e : dept._2) {
				list.add(new Tuple2<>(dept._1, new Tuple2<>(e._1.getName(), e._2)));
			}
			return list.iterator();
		});

		for (Tuple2<String, Tuple2<String, Integer>> d : flat.collect()) {
			System.out.println(d);
		}

		printSection("RDD TRANSFORMATIONS ENDED");
	}

	// =========================
	// FORMATTING HELPERS
	// =========================
	private static void printDivider() {
		System.out.println("\n------------------------------------------------------------\n");
	}

	private static void printSection(String title) {
		System.out.println("\n==================== " + title + " ====================\n");
	}

	private static void printSubSection(String title) {
		System.out.println("\n---------- " + title + " ----------\n");
	}

	private static void printBanner(String title) {
		System.out.println("\n************************************************************");
		System.out.println("**** " + title + " ****");
		System.out.println("************************************************************\n");
	}
}
