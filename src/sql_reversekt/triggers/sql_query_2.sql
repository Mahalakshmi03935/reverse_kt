CREATE TRIGGER trg_emp
on employee
AFTER INSERT
AS
BEGIN
insert into employee_audit
select emp_id,'INS',GETDATE()
FROM inserted
END