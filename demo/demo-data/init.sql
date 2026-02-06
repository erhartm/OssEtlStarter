-- Sample SQL Server data initialization
CREATE TABLE testtable (
    id INT PRIMARY KEY,
    name NVARCHAR(50),
    value FLOAT
);
INSERT INTO testtable (id, name, value) VALUES (1, 'Alice', 10.5);
INSERT INTO testtable (id, name, value) VALUES (2, 'Bob', 20.0);
