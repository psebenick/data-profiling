-- generate SQL for getting column stats

declare @tabcolcursor CURSOR;
declare @tab nvarchar(max);
declare @col nvarchar(max);
declare @datatype nvarchar(max);
declare @dynamicsql nvarchar(max);

create table #colstats( tab NVARCHAR(MAX), col NVARCHAR(MAX), datatype NVARCHAR(MAX), num_distinct int, num_null int, min_val NVARCHAR(MAX), max_val NVARCHAR(MAX));

set @tabcolcursor = cursor for
SELECT --top 10 
 t.name ,
 c.name ,
 tt.name  
from sys.all_columns c
join sys .all_objects  t on  (c.object_id=t.object_id and t.type_desc='USER_TABLE')
join sys.types tt on (c.system_type_id=tt.user_type_id)
where
t.name  like 'CS%'
order by t.name,c.column_id

open @tabcolcursor
fetch next from @tabcolcursor into @tab, @col, @datatype
while @@FETCH_STATUS = 0 
begin
	  set @dynamicsql =   'insert into #colstats select ' +
	 '''' + @tab + ''' tab,' +
	 '''' + @col + ''' col,' +
	 '''' + @datatype + ''' datatype,' + 
	'count(distinct ' + @col + ') num_distict, ' +
	'sum(case when ' +  @col + ' is null then 1 else 0 end) num_null, ' +
	--'sum(case when cast(' + @col + ' as varchar(255)) =''?'' then 1 else 0 end) num_q, ' +
	--'sum(case when cast(' + @col + ' as varchar(255)) ='''' then 1 else 0 end) num_e, ' +
	--'sum(case when cast(' + @col + ' as varchar(255)) =''NOKEY'' then 1 else 0 end) num_nk, ' +
	--'max(len(' + @col + ')) max_len, ' +
	'cast(min(' + @col + ') as varchar(255)) min_val, ' +
	'cast(max(' + @col + ') as varchar(255)) max_val' +
	' from '  + @tab + ';'

   print @dynamicsql
   exec (@dynamicsql);
   fetch next from @tabcolcursor into @tab, @col, @datatype;
end
CLOSE @tabcolcursor;
DEALLOCATE @tabcolcursor;
select * from #colstats;

--drop table #colstats;
