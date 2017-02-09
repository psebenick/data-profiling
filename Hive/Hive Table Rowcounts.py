%pyspark
print sc.version
### get count of records for each table

dftables = sqlContext.tables('default')
#print dftables.show()
print "Database  Tables and Row Counts"
df1 = sqlContext.sql("Use default")
for t in dftables.collect():
    #print t.tableName
    query = "select " + '"' +t.tableName + '"' + " as tableName, count(*) as num_rows from " + t.tableName
    #print query
    df2 = sqlContext.sql(query)
    for r in df2.collect():
        print r.tableName + '|' , r.num_rows
