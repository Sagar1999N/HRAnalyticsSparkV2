package com.sagar.hranalyticsv2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.sagar.pojo.Employee;

import scala.Tuple2;

public class HRAnalyticsV2 {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("HRAnalyticsV2").getOrCreate();
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<String> emps = javaSparkContext.textFile("./data/input/*.csv");
		for (String e : emps.collect()) {
			System.out.println(e);
		}

		String header = emps.first();
		System.out.println(header);

		emps = emps.filter(x -> !(x.equals(header)));

		JavaRDD<String[]> data = emps.map(x -> x.split(","));
		for (String[] d : data.collect()) {
			for (String id : d) {
				System.out.print(id + ",");
			}
			System.out.println();
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		JavaRDD<Employee> employees = data.map(parts -> new Employee(Integer.parseInt(parts[0]), parts[1], parts[2],
				Integer.parseInt(parts[3]), sdf.parse(parts[4])));

		for (Employee d : employees.collect()) {
			System.out.println(d.toString());
		}

		System.out.println("richEmployees");

		// TODO
		// Rich Employees : having salary greater than 5000
		JavaRDD<Employee> richEmployees = employees.filter(e -> e.getSalary() > 5000);

		for (Employee d : richEmployees.collect()) {
			System.out.println(d.toString());
		}

		// TODO
		// Avg Salary : Average salary of employees per department
		JavaPairRDD<String, Integer> deptSalaryPairs = employees
				.mapToPair(e -> new Tuple2<>(e.getDepartment(), e.getSalary()));

		for (Tuple2<String, Integer> d : deptSalaryPairs.collect()) {
			System.out.println(d.toString());
		}

		JavaPairRDD<String, Tuple2<Integer, Integer>> deptSumCount = deptSalaryPairs
				.mapValues(sal -> new Tuple2<>(sal, 1));// .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 +
														// b._2));

		for (Tuple2<String, Tuple2<Integer, Integer>> d : deptSumCount.collect()) {
			System.out.println(d.toString());
		}

		JavaPairRDD<String, Tuple2<Integer, Integer>> deptSumCountR = deptSumCount
				.reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

		for (Tuple2<String, Tuple2<Integer, Integer>> d : deptSumCountR.collect()) {
			System.out.println(d.toString());
		}

		JavaPairRDD<String, Double> deptSumCountAvg = deptSumCountR
				.mapValues(sc -> Double.valueOf(sc._1) / Double.valueOf(sc._2));

		for (Tuple2<String, Double> d : deptSumCountAvg.collect()) {
			System.out.println(d.toString());
		}

		// TODO
		// Top Earners : Top 2 earners per department
		JavaPairRDD<String, Tuple2<Employee, Integer>> empSalaryPairs = employees
				.mapToPair(e -> new Tuple2<>(e.getDepartment(), new Tuple2<>(e, e.getSalary())));

		for (Tuple2<String, Tuple2<Employee, Integer>> d : empSalaryPairs.collect()) {
			System.out.println(d.toString());
		}

		JavaPairRDD<String, Iterable<Tuple2<Employee, Integer>>> empSalaryPairsGroup = empSalaryPairs.groupByKey();

		JavaPairRDD<String, Iterable<Tuple2<Employee, Integer>>> top2perDept = empSalaryPairsGroup.mapValues(itr -> {
			List<Tuple2<Employee, Integer>> list = new ArrayList<>();
			itr.forEach(list::add);
			list.sort((a, b) -> b._2.compareTo(a._2));
//			return list.subList(0, Math.min(2, list.size()));
			return new ArrayList<>(list.subList(0, Math.min(2, list.size())));

		});

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

		for (Tuple2<String, Tuple2<String, Integer>> d : flat.collect()) {
			System.out.println(d);
		}
	}
}
