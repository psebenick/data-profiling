-- generate SQL for getting list of values or column distribution 
-- note use of temp table #colstats generated earlier

declare @DynamicSQL2 NVARCHAR(MAX)
set @DynamicSQL2 = ''
create table #coldistribution( tab NVARCHAR(MAX), col NVARCHAR(MAX), colval NVARCHAR(MAX), num_rows int);


SELECT   @DynamicSQL2 = @DynamicSQL2 +  
'insert into #coldistribution select ' +
 '''' + tab + ''' tab,' +
 '''' + col + ''' col,' + 
 + col + ',' + 
 'count(*) as num_rows from ' + tab +  ' group by ' + col
 + CHAR(13) + ';'  + CHAR(13) 

from #colstats where num_distinct between 2 and 20;  -- can define maximum cardinality here

SELECT CAST(@DynamicSQL2 AS XML);
execute(@DynamicSQL2);
select * from #coldistribution;

--drop table #colstats;
--drop table #coldistribution;
go

drop table #coldistribution;
