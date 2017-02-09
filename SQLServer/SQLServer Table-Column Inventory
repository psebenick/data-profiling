select  t.name,c.name as column_name,column_id,tt.name as datatype, c.max_length
 from sys.all_columns c
join sys.all_objects  t on  (c.object_id=t.object_id and t.type_desc='USER_TABLE')
join sys.types tt on (c.system_type_id=tt.user_type_id)
order by t.name,c.column_id
