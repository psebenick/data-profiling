select * from sys.databases;
select * from information_schema.tables;



CREATE TABLE AllTables ([DB Name] sysname, [Schema Name] sysname, [Table Name] sysname)
DECLARE @SQL NVARCHAR(MAX)
SELECT @SQL = COALESCE(@SQL,'') + '
insert into AllTables
select ' + QUOTENAME(name,'''') + ' as [DB Name], [Table_Schema] as [Table Schema], [Table_Name] as [Table Name] from ' +
QUOTENAME(Name) + '.INFORMATION_SCHEMA.Tables;' FROM sys.databases where name not in ('model','master','msdb')
ORDER BY name
 print @SQL
EXECUTE(@SQL)

SELECT * FROM AllTables ORDER BY [DB Name],[SCHEMA NAME], [Table Name]
