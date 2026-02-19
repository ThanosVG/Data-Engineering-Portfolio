-- Day 3: Employee Salary Analysis

-- Aggregation: Average salary
SELECT AVG(Current_Salary) AS avg_salary FROM employees;

-- Top earners: Highest salaries with details
SELECT Employee_name, Employee_age, Current_Salary, Department FROM employees ORDER BY Current_Salary DESC LIMIT 10;

-- Group by city: Avg salary per city
SELECT Department, AVG(Current_Salary) AS avg_dept_salary FROM employees GROUP BY Department HAVING AVG(Current_Salary) > 50000;

-- Subquery: Employees above average salary
SELECT Employee_name, Current_Salary FROM employees WHERE Current_Salary > (SELECT AVG(Current_Salary) FROM employees);