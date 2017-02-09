-- Hive SQL -- show tables
-- connect to hive metastore mySQL database -- this contains the data dictionary for hive
-- https://issues.apache.org/jira/secure/attachment/12471108/HiveMetaStore.pdf
-- mysql -h sandbox.hortonworks.com -P 3306 -u root
use hive;
select
T.TBL_ID, T.OWNER, T.TBL_NAME, T.TBL_TYPE 
from
TBLS as T
  
select
T.TBL_ID, T.OWNER, T.TBL_NAME, T.TBL_TYPE,
TP.numFiles, TP.numRows, TP.totalSize, TP.rawDataSize, TP.STATS,
S.SD_ID,S.CD_ID,SUBSTR(S.OUTPUT_FORMAT,30,20) OFORMAT, S.LOCATION
from
  TBLS as T
  join SDS S on T.SD_ID=S.SD_ID
  join -- pivot table_params from rows to columns
  (select TBL_ID,
   max(case when PARAM_KEY='numFiles' then PARAM_VALUE else null end) numFiles,
   max(case when PARAM_KEY='numRows' then PARAM_VALUE else null end) numRows,
   max(case when PARAM_KEY='totalSize' then PARAM_VALUE else null end) totalSize,
   max(case when PARAM_KEY='rawDataSize' then PARAM_VALUE else null end) rawData Size,
   max(case when PARAM_KEY='COLUMN_STATS_ACCURATE' then PARAM_VALUE else null end) STATS
   from TABLE_PARAMS
   group by TBL_ID) as TP
   on T.TBL_ID=TP.TBL_ID;

#partitioned tables
select
T.TBL_ID, T.OWNER, T.TBL_NAME, T.TBL_TYPE, P.PART_ID,
TP.numFiles, TP.numRows, TP.totalSize, TP.rawDataSize,
# TP.STATS, S.SD_ID,S.CD_ID,
SUBSTR(S.OUTPUT_FORMAT,30,20) OFORMAT, S.LOCATION
from
  TBLS as T
  join PARTITIONS P on T.TBL_ID=P.TBL_ID
  join SDS S on P.SD_ID=S.SD_ID
  join -- pivot table_params from rows to columns
  (select PART_ID,
   max(case when PARAM_KEY='numFiles' then PARAM_VALUE else null end) numFiles,
   max(case when PARAM_KEY='numRows' then PARAM_VALUE else null end) numRows,
   max(case when PARAM_KEY='totalSize' then PARAM_VALUE else null end) totalSize,
   max(case when PARAM_KEY='rawDataSize' then PARAM_VALUE else null end) rawDataSize,
   max(case when PARAM_KEY='COLUMN_STATS_ACCURATE' then PARAM_VALUE else null end) STATS
   from PARTITION_PARAMS
   group by PART_ID) as TP
   on P.PART_ID=TP.PART_ID;
