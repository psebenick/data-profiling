-- Hive SQL -- show tables

-- connect to hive metastore mySQL database -- this contains the data dictionary for hive
select
T.TBL_ID, T.OWNER, T.TBL_NAME, T.TBL_TYPE 
from
TBLS as T
