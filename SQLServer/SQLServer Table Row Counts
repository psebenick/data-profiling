DECLARE  @DynamicSQL NVARCHAR(MAX)
set @DynamicSQL=''
-- create a sql script that contains a select count(*) from each table

SELECT   @DynamicSQL = @DynamicSQL +
           'SELECT ' + quotename(table_schema,'''') + ' as [Schema Name], ' +
           QUOTENAME(TABLE_NAME,'''') + 
           ‘ as [Table Name], COUNT(*) AS [Records Count] FROM ' + 
           quotename(Table_schema) + '.' + QUOTENAME(TABLE_NAME)
					+ CHAR(13) + ' UNION ALL ' + CHAR(13)
FROM  INFORMATION_SCHEMA.TABLES
where table_name like ‘P%’ 
ORDER BY TABLE_NAME

set @DynamicSQL = @DynamicSQL + Char(13) + 'Select ''1'',''1'',''1''  ' -- need to account for that last union all
 
--print (@DynamicSQL) -- we may want to use PRINT to debug/edit the SQL
SELECT CAST(@DynamicSQL AS XML)
EXEC( @DynamicSQL)
go
