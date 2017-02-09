%pyspark
print sc.version
## collect and display Hive Metastore data.  Written for Zeppelin

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import split
from pyspark.sql.functions import lit
from pyspark.sql.functions import trim
from pyspark.sql.functions import first
from pyspark.sql.functions import instr
from pyspark.sql.functions import substring
from pyspark.sql.functions import length
from pyspark.sql.functions import ltrim
from pyspark.sql.functions import rtrim
from pyspark.sql.functions import explode
from pyspark.sql.functions import col

#Create an empty RDD to eventually hold ALL the hive metastore tables and properties
schema=StructType([\
StructField("Name",StringType(),False),\
StructField("Value",StringType(),False),\
StructField("Tname",StringType(),False),\
StructField("DBname",StringType(),False)\
])
rdd4 = sqlContext.createDataFrame(sc.emptyRDD(),schema).rdd

#Create an empty RDD to eventually hold ALL the hive tables and columns
TCschema=StructType([\
StructField("col_name",StringType(),False),\
StructField("data_type",StringType(),False),\
StructField("comment",StringType(),False),\
StructField("Tname",StringType(),False),\
StructField("DBname",StringType(),False)\
])
rddTC1 = sqlContext.createDataFrame(sc.emptyRDD(),TCschema).rdd

d_databases = sqlContext.sql("show databases")
for dbrow in d_databases.collect():  ### loop over each database
    #print "Database: " + dbrow.result
    dfdd = sqlContext.sql("Use " + dbrow.result)
    d_tables = sqlContext.sql("show tables")      
    for tblrow in d_tables.collect():      ### loop over table each table
        table_name = tblrow.tableName 
        if tblrow.isTemporary :
            print table_name + " Temporary"  ## temp tables don't have any properties
        else :
            #print table_name

            df1 = sqlContext.sql("describe formatted " + tblrow.tableName )
            
            # get list of table properties, filter out columns and split into name value pairs and put in RDD filter out columns
            df2 = df1.filter("result like '%:%'").select( trim(split("result",'\t')[0]).alias("Name") ,split("result",'\t')[1].alias("Value"))\
            .withColumn("Name",func.regexp_replace('Name',":",""))\
            .withColumn('Tname', lit(tblrow.tableName)).withColumn('DBname', lit(dbrow.result))
            
            # get table stats and append to table properties
            df4 = sqlContext.sql("show tblproperties " + tblrow.tableName ) 
            df5 = df4.select( split("result",'\t')[0].alias("Name") ,split("result",'\t')[1].alias("Value"))\
            .withColumn('Tname', lit(tblrow.tableName)).withColumn('DBname', lit(dbrow.result))
            
            df6 = df2.unionAll(df5)
            
            rdd4 = rdd4.union(df6.rdd)  #append each tables properties to the whole list of tables and properties
            
            # get list of table columns
            dfc1 = sqlContext.sql("describe " + tblrow.tableName )
            dfc2 = dfc1.filter("instr(col_name,'#')=0").withColumn('Tname', lit(tblrow.tableName)).withColumn('DBname', lit(dbrow.result))
            rddTC1 = rddTC1.union(dfc2.rdd)
            
            # get partition information
            if df1.filter("result like '# Partition%'").count()>0:
                dfp1 = sqlContext.sql("show table extended like "+ tblrow.tableName)
                dfp2 = dfp1.filter("result like 'partition%'").select( trim(split("result",':')[0]).alias("Name") ,split("result",':')[1].alias("Value")  )
                dfp3 = dfp2.withColumn('Tname', lit(tblrow.tableName)).withColumn('DBname', lit(dbrow.result))
                rdd4 = rdd4.union(dfp3.rdd)   # append this to the list of table properties
                #print dfp3.show(20,False)
                
                #dfpc = dfp1.filter("result like 'partitionColumns%'").select(substring("result",45,100).alias('s')).select(explode(split("s",",")).alias('pc'))
                #dfpc = dfpc.withColumn("pc",func.regexp_replace('pc',"}",""))
                #dfpc = dfpc.withColumn("pc",func.regexp_replace('pc',"^[\s]",""))
                #dfpc1 = dfpc.select(split("pc"," ")[1].alias("PartitionColumn"))
                #dfpc1 = dfpc1.withColumn('Tname', lit(tblrow.tableName)).withColumn('DBname', lit(dbrow.result)).withColumn('isPartitioned',lit(1))
                
                #xxx = dfc2.alias('a').join(dfpc1.alias('b'),(col('a.col_name')==col('b.PartitionColumn')) ,'left_outer' ).select('a.*')
                #select (["dfc2."+xx for xx in dfc2.columns] + ['dfpc1.isPartitioned'])
               
                #print xxx.show(50,False)
                
#display database and table properties            
df7 = sqlContext.createDataFrame(rdd4)
### pivot the list of properties so that the properties are displayed as columns
df8 = df7.groupBy("DBname","Tname").pivot("Name").agg(first("Value"))
print "Possible fields to select for each table"
df8.printSchema()
#print df8.show()
#print df8.collect()
#print df8.select("*").show()
print df8.select("DBname","Tname","Owner","Table Type","Location","numRows","numFiles","rawDataSize").sort("DBname","Tname").show(50,False)
#for row in df8.collect():
#    print row.Tname + ' | ' +row.Location

#display table columns
dfc2 = sqlContext.createDataFrame(rddTC1).distinct().orderBy(["DBname","Tname"])
print dfc2.show(1000)
