-- Create STG_HCM_MAXEFFDT_ACTRSN Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 2/16/2017

set hive.execution.engine=tez;

DROP TABLE STG_HCM_MAXEFFDT_ACTRSN;

CREATE TABLE STG_HCM_MAXEFFDT_ACTRSN
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
) stored as orc;

---------------------------------
-- The Following Command will then populate the TABLE
---------------------------------

INSERT INTO STG_HCM_MAXEFFDT_ACTRSN
(
  action
, action_reason
, effdt
, eff_status
, descr
, descrshort
, objectownerid
, system_data_flg
, lastupddttm
, lastupdoprid
, dw_source_db
)
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