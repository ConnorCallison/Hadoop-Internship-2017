-- Create STG_HCM_MAXEFFDT_ACTRSN_SPARK Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 2/16/2017

DROP TABLE IF EXISTS STG_HCM_MAXEFFDT_ACTRSN_SPARK;

CREATE TABLE STG_HCM_MAXEFFDT_ACTRSN_SPARK
(
  `ACTION`          String
, `ACTION_REASON `  String
, `EFFDT`           TIMESTAMP
, `EFF_STATUS`      String
, `DESCR`           String
, `DESCRSHORT`      String
, `OBJECTOWNERID`   String
, `SYSTEM_DATA_FLG` String
, `LASTUPDDTTM`     TIMESTAMP
, `LASTUPDOPRID`    String
, `DW_SOURCE_DB`          String
);

---------------------------------
-- The Following Command will then populate the TABLE
---------------------------------

INSERT INTO STG_HCM_MAXEFFDT_ACTRSN_SPARK
SELECT
  l.ACTION
, l.ACTION_REASON
, l.EFFDT
, l.EFF_STATUS
, l.DESCR
, l.DESCRSHORT
, l.OBJECTOWNERID
, l.SYSTEM_DATA_FLG
, l.LASTUPDDTTM
, l.LASTUPDOPRID
, 'DW_COMMON.DB_CMS_HR'

FROM
  PS_ACTN_REASON_TBL  L,
  (SELECT MAX(L_ED.EFFDT) as max_effdt, L_ED.ACTION_REASON as act_rsn, L_ED.ACTION as action
   FROM PS_ACTN_REASON_TBL L_ED
   WHERE L_ED.EFFDT <= from_unixtime(unix_timestamp())
   GROUP BY  L_ED.ACTION, L_ED.ACTION_REASON) maxresult

WHERE  
    L.EFFDT = maxresult.max_effdt
    AND L.ACTION_REASON = maxresult.act_rsn
    AND L.ACTION = maxresult.action;