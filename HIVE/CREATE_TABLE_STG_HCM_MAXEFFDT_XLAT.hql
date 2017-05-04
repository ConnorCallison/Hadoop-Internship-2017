-- Create STG_HCM_MAXEFFDT_XLAT Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 2/21/2017

set hive.execution.engine=tez;

DROP TABLE STG_HCM_MAXEFFDT_XLAT;

CREATE TABLE STG_HCM_MAXEFFDT_XLAT
(
  `FIELDNAME`   String
, `FIELDVALUE`  String
, `EFFDT`       TIMESTAMP
, `EFF_STATUS`  String
, `XLATLONGNAME`    String
, `XLATSHORTNAME`   String
, `LASTUPDDTTM` TIMESTAMP
, `LASTUPDOPRID`    String
, `SYNCID`      Int
, `DW_SOURCE_DB`  String
) stored as orc;

--------------
--We can then populate the table from:
-- psxlatitem
--------------

INSERT INTO STG_HCM_MAXEFFDT_XLAT
(
  `fieldname`
, `fieldvalue`
, `effdt`
, `eff_status`
, `xlatlongname`
, `xlatshortname`
, `lastupddttm`
, `lastupdoprid`
, `syncid`
, `dw_source_db`
)
SELECT
  l.FIELDNAME
, l.FIELDVALUE
, l.EFFDT
, l.EFF_STATUS
, l.XLATLONGNAME
, l.XLATSHORTNAME
, l.LASTUPDDTTM
, l.LASTUPDOPRID
, l.SYNCID
, 'HHUMPRD'

FROM
  PSXLATITEM  L,
  (SELECT MAX(L_ED.EFFDT) as max_effdt, L_ED.FIELDNAME as f_name, L_ED.FIELDVALUE as f_value
    FROM PSXLATITEM L_ED
    WHERE L_ED.EFFDT <= from_unixtime(unix_timestamp())
    GROUP BY L_ED.FIELDNAME, L_ED.FIELDVALUE) max_result

WHERE  
    l.EFFDT = max_result.max_effdt
    AND l.FIELDNAME = max_result.f_name
    AND l.FIELDVALUE = max_result.f_value;