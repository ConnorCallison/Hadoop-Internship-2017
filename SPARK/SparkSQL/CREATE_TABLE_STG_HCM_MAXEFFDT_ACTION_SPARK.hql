-- Create STG_HCM_MAXEFFDT_ACTION Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 12/16/2017

--set hive.execution.engine=tez;

DROP TABLE IF EXISTS STG_HCM_MAXEFFDT_ACTION_SPARK;

CREATE TABLE STG_HCM_MAXEFFDT_ACTION_SPARK
(
`ACTION`          String,
`EFFDT`           TIMESTAMP,
`EFF_STATUS`      String,
`ACTION_DESCR`    String,
`ACTION_DESCRSHORT`   String,
`OBJECTOWNERID`   String,
`LASTUPDDTTM`     TIMESTAMP,
`LASTUPDOPRID`    String,
`SYSTEM_DATA_FLG` String,
`DW_SOURCE_DB`          String
);

---------------------------------
-- The Following Command will then populate the TABLE
---------------------------------

INSERT INTO STG_HCM_MAXEFFDT_ACTION_SPARK
SELECT
  l.ACTION
, l.EFFDT
, l.EFF_STATUS
, l.ACTION_DESCR
, l.ACTION_DESCRSHORT
, l.OBJECTOWNERID
, l.LASTUPDDTTM
, l.LASTUPDOPRID
, l.SYSTEM_DATA_FLG
, 'DW_COMMON.DB_CMS_HR'

FROM
  PS_ACTION_TBL  L,
  (SELECT MAX(L_ED.EFFDT) as max_effdt, L_ED.ACTION as action
    FROM PS_ACTION_TBL L_ED
    WHERE L_ED.EFFDT <= from_unixtime(unix_timestamp())
    GROUP BY L_ED.ACTION) maxresult
WHERE  
    L.EFFDT = maxresult.max_effdt
    AND L.ACTION = maxresult.action;
         