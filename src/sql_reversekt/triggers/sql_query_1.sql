create database sqltrig
use sqltrig
 
CREATE TABLE employee (
    emp_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    hire_date DATE,
    job_title VARCHAR(50),
    salary DECIMAL(10, 2),
    manager_id INT,
    department_id INT
);
 
INSERT INTO employee (emp_id, first_name, last_name, email, phone, hire_date, job_title, salary, manager_id, department_id)
VALUES
    (1, 'John', 'Doe', 'john.doe@company.com', '555-1234', '2020-01-01', 'Software Engineer', 80000.00, NULL, 1),
    (2, 'Jane', 'Smith', 'jane.smith@company.com', '555-5678', '2018-03-15', 'Project Manager', 95000.00, 1, 2),
    (3, 'Michael', 'Johnson', 'michael.johnson@company.com', '555-9012', '2021-07-01', 'Data Analyst', 70000.00, 2, 3),
    (4, 'Emily', 'Williams', 'emily.williams@company.com', '555-3456', '2019-11-01', 'Marketing Specialist', 65000.00, 2, 4),
    (5, 'David', 'Brown', 'david.brown@company.com', '555-7890', '2022-02-01', 'Accountant', 75000.00, NULL, 5),
    (6, 'Sarah', 'Davis', 'sarah.davis@company.com', '555-2345', '2017-09-01', 'HR Manager', 90000.00, 1, 6),
    (7, 'Robert', 'Miller', 'robert.miller@company.com', '555-6789', '2020-05-15', 'Sales Representative', 60000.00, 2, 4),
    (8, 'Jessica', 'Wilson', 'jessica.wilson@company.com', '555-0123', '2018-08-01', 'Web Developer', 85000.00, 1, 1),
    (9, 'Christopher', 'Anderson', 'christopher.anderson@company.com', '555-4567', '2021-03-01', 'Network Administrator', 75000.00, 2, 7),
    (10, 'Amanda', 'Taylor', 'amanda.taylor@company.com', '555-8901', '2019-06-01', 'Customer Service Representative', 50000.00, 6, 8);
 
	select * from employee
 
	create table employee_audit
	(
	employee_id int,
	operation varchar(10),
	updatedDate Datetime
	)
 
	INSERT INTO employee 
VALUES
    (12, 'Maha', 'Lakhsmi', 'brml@company.com', '777-3848', '2024-03-04', 'Data Engineer', 80000.00, 2, 1)


  
	INSERT INTO employee 
VALUES
    (12, 'Maha', 'Li', 'brml@company.com', '777-3848', '2024-03-07', 'Data Engineer', 80000.00, 1, 1)


select * from employee_audit

