--select * from sys.databases;
select
SERVERPROPERTY('ServerName') Servername,
db_name() DB_name,
SERVERPROPERTY('InstanceName') AS [Instance], 
SERVERPROPERTY('Edition') AS [Edition],
SERVERPROPERTY('ProductVersion') AS [ProductVersion]; 

select * from sys.databases;

sp_spaceused -- 515,931,352 KB
DBCC PDW_SHOWSPACEUSED ( "ssplbdp01.E_IL1.benchmark_cc_detail" );  

select * from sys.pdw_nodes_tables

 drop view Admin.vtablesize;


--synapse
drop view admin.vtabledistribution;

CREATE VIEW Admin.vTableDistribution as
SELECT
SERVERPROPERTY('ServerName') Server_Name,
db_name() as Database_Name
,s.name as Schema_Name
,t.name as  Table_Name
,t.object_id as Table_Object_Id
,tp.distribution_policy_desc as  Dist_Policy_Name
,nt.object_id as node_table_ojbect_id,nt.pdw_node_id,nt.distribution_id,nt.modify_date,nt.type_desc
,tm.physical_name
,nps.partition_number
,nps.index_id --  ID of the heap or index the partition is part of.0 = Heap  1 = Clustered index. > 1 = Nonclustered index
,case when row_number() over (partition by nps.object_id,nps.partition_number,nps.distribution_id order by nps.index_id)=1 then nps.row_count else 0 end  as row_count
--,nps.row_count
,nps.reserved_page_count/128.0 as reserved_space_mb  
,nps.used_page_count/128.0 as used_space_mb
from
sys.schemas s
JOIN sys.tables t    ON s.[schema_id] = t.[schema_id]
JOIN sys.pdw_table_distribution_properties tp    ON t.[object_id] = tp.[object_id]
JOIN sys.pdw_table_mappings tm    ON t.[object_id] = tm.[object_id]
JOIN sys.pdw_nodes_tables nt    ON tm.[physical_name] = nt.[name]
--JOIN sys.dm_pdw_nodes pn    ON  nt.[pdw_node_id] = pn.[pdw_node_id]
--JOIN sys.pdw_distributions di    ON  nt.[distribution_id] = di.[distribution_id]
JOIN sys.dm_pdw_nodes_db_partition_stats nps ON nt.[object_id] = nps.[object_id]    AND nt.[pdw_node_id] = nps.[pdw_node_id]    AND nt.[distribution_id] = nps.[distribution_id]

select * from admin.vtabledistribution where table_name = ;

select table_name,count(*),sum(reserved_space_mb) from admin.vtabledistribution where table_name='XXXX' group by table_name;


drop view admin.vtablesize;
Create view Admin.vTableSize
as
Select
Server_Name,
Database_Name,
Schema_Name,
Table_Name,
Table_Object_Id,
Dist_Policy_Name,
count(*) as Distribution_Cnt,sum(row_count) as Row_cnt, sum(reserved_space_mb) Reserved_Space_MB , sum(used_space_mb) Used_Space_MB
from Admin.vTableDistribution
group by 
Server_name,Database_Name,schema_name,table_name,table_object_id,dist_policy_name;

grant view database state to lbdp_sql_developer; ---  needed for developers to query sys.pdw tables and tablesize

select * from Admin.vtablesize 
--where row_cnt>0 
order by 1,2


/* hash columns */

SELECT   SERVERPROPERTY('ServerName') Servername,
    OBJECT_SCHEMA_NAME(tdp.object_id) schemaName,
    OBJECT_NAME(tdp.object_id) tableName,
	tdp.distribution_policy_desc,
    c.name AS hashDistributionColumnName,
    cdp.distribution_ordinal
FROM sys.pdw_table_distribution_properties tdp
        INNER JOIN sys.pdw_column_distribution_properties cdp ON tdp.object_id = cdp.object_id
            INNER JOIN sys.columns c ON cdp.object_id = c.object_id
                AND cdp.column_id = c.column_id
WHERE tdp.distribution_policy_desc = 'HASH'
  AND cdp.distribution_ordinal > 0
  order by 1,2,3

