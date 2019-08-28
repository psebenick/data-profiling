CREATE TABLE #SpaceUsed (
	 TableName sysname
	,NumRows BIGINT
	,ReservedSpace VARCHAR(50)
	,DataSpace VARCHAR(50)
	,IndexSize VARCHAR(50)
	,UnusedSpace VARCHAR(50)
	) ;

DECLARE @str VARCHAR(500)
SET @str =  'exec sp_spaceused ''?'''
INSERT INTO #SpaceUsed 
EXEC sp_msforeachtable @command1=@str

SELECT distinct * FROM #SpaceUsed ORDER BY TableName;
drop table #SpaceUsed;



SELECT 
    t.NAME AS TableName,
    s.Name AS SchemaName,
    p.rows AS RowCounts,
    CAST(((SUM(a.total_pages) * 8) / 1024.00) AS NUMERIC(36, 2)) AS TotalSpaceMB,
    CAST(((SUM(a.used_pages) * 8) / 1024.00) AS NUMERIC(36, 2)) AS UsedSpaceMB
FROM 
    sys.tables t
INNER JOIN  sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN  sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN  sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE 
    t.NAME NOT LIKE 'dt%' 
    AND t.is_ms_shipped = 0
    AND i.OBJECT_ID > 255 
GROUP BY   t.Name, s.Name, p.Rows
ORDER BY   t.Name
