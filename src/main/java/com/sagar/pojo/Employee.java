package com.sagar.pojo;

import java.io.Serializable;
import java.util.Date;

public class Employee implements Serializable {

	private Integer employee_id;
	private String name;
	private String department;
	private Integer salary;
	private Date joining_date;

	public Integer getEmployee_id() {
		return employee_id;
	}

	public void setEmployee_id(Integer employee_id) {
		this.employee_id = employee_id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDepartment() {
		return department;
	}

	public void setDepartment(String department) {
		this.department = department;
	}

	public Integer getSalary() {
		return salary;
	}

	public void setSalary(Integer salary) {
		this.salary = salary;
	}

	public Date getJoining_date() {
		return joining_date;
	}

	public void setJoining_date(Date joining_date) {
		this.joining_date = joining_date;
	}

	public Employee(Integer employee_id, String name, String department, Integer salary, Date joining_date) {
		super();
		this.employee_id = employee_id;
		this.name = name;
		this.department = department;
		this.salary = salary;
		this.joining_date = joining_date;
	}

	@Override
	public String toString() {
		return "Employee [employee_id=" + employee_id + ", name=" + name + ", department=" + department + ", salary="
				+ salary + ", joining_date=" + joining_date + "]";
	}

}
