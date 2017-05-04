-- Create STG_HCM_PRE_COMP_SPARK Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 2/23/2017
--
--cast(CONCAT(date_format(cast(c.effdt as date),'YYYYMMdd'), lpad(cast(cast(c.effseq as int) as string), 4, '0'), c.EMPLID, lpad(cast(cast(c.EMPL_RCD as int) as string), 4, '0')) as decimal(38,0)) as job_dayseq_key

--set hive.execution.engine=tez;

drop table IF EXISTS STG_HCM_PRE_COMP_SPARK;

CREATE TABLE STG_HCM_PRE_COMP_SPARK_TEMP (
  `JOB_DAYSEQ_KEY`          DECIMAL(38,0)
, `DW_COMP_LN_CNT`          Int
, `EMPLID`                  String
, `EMPL_RCD`                Int
, `EFFDT`                   TIMESTAMP 
, `EFFSEQ`                  Int
, `COMP_EFFSEQ`             Int
, `COMP_RATECD`             String
, `COMP_RATECD_DESCR`       String 
, `COMP_RATECD2`            String
, `COMP_RATECD_DESCR2`      String 
, `COMP_RATE_POINTS`        Int
, `COMPRATE`                FLOAT
, `UNITS`                   FLOAT
, `COMP_PCT`                FLOAT
, `COMP_FREQUENCY`          String
, `COMP_FREQUENCY_DESCR`    String 
, `CURRENCY_CD`             String
, `MANUAL_SW`               String
, `CONVERT_COMPRT`          FLOAT
, `RATE_CODE_GROUP`         String
, `RATE_CODE_GROUP_DESCR`   String 
, `CHANGE_AMT`              FLOAT
, `CHANGE_PCT`              FLOAT 
, `CHANGE_PTS`              Int
, `FTE_INDICATOR`           String
, `CMP_SRC_IND`             String
, `CMP_SRC_IND_DESCR`       String 
, `DW_JOB_ID`               String        
, `DW_JOBSEQ`               BIGINT        
, `DW_START_DATE`           TIMESTAMP       
, `DW_END_DATE`             TIMESTAMP       
, `DW_CURRENT_IND`          String        
, `DW_SOURCE_DB`            String        
, `DW_CF_YR`                Int       
, `DW_CREATED_EW_DTTM`      TIMESTAMP       
, `DW_LASTUPD_EW_DTTM`      TIMESTAMP
);       

-- 2013123100000110995270100 2012-12-31 00:00:00 0 11099527 100
-- cast(CONCAT(date_format(TO_DATE(substr('2012-12-31 00:00:00',1,10)), 'YYYYMMdd'), lpad(cast(cast(0 as int) as string), 4, '0'), 11099527, lpad(cast(cast(100 as int) as string), 4, '0')) as decimal(38,0));
INSERT INTO STG_HCM_PRE_COMP_SPARK_TEMP 
SELECT distinct
cast(CONCAT(date_format(TO_DATE(substr(c.effdt,1,10)), 'yyyyMMdd'), lpad(cast(cast(c.effseq as int) as string), 4, '0'), c.EMPLID, lpad(cast(cast(c.EMPL_RCD as int) as string), 4, '0')) as decimal(38,0)) as job_dayseq_key
, count(*) over (partition by c.emplid,c.empl_rcd, c.effdt, c.effseq) as dw_comp_ln_cnt
, c.EMPLID
, c.EMPL_RCD
, c.EFFDT
, c.EFFSEQ
, c.COMP_EFFSEQ
, c.COMP_RATECD
, '-'
, '-'
, '-'
, c.COMP_RATE_POINTS
, c.COMPRATE
, cast(null as int)
, c.COMP_PCT
, c.COMP_FREQUENCY
, '-'
, c.CURRENCY_CD
, c.MANUAL_SW
, c.CONVERT_COMPRT
, c.RATE_CODE_GROUP
, '-'
, c.CHANGE_AMT
, c.CHANGE_PCT
, c.CHANGE_PTS
, c.FTE_INDICATOR
, c.CMP_SRC_IND
, NVL(X1.XLATLONGNAME,'-')
, CONCAT(c.EMPLID, '-', lpad(cast(cast(c.EMPL_RCD as int) as string), 4, '0')) as dw_job_id
, dense_rank() over (partition by c.emplid,c.empl_rcd order by c.effdt desc,c.effseq desc) as dw_jobseq
, c.EFFDT as dw_start_date
, LEAD(c.EFFDT,1,TO_DATE('2099-6-30')) OVER (PARTITION BY c.EMPLID, c.EMPL_RCD ORDER BY c.effdt, c.effseq) as dw_end_date
, '-'
, 'DW.DB_CMS_HR'
,  CASE D_DAT.FISCAL_YEAR
        when 2005 then D_DAT.FISCAL_YEAR
        when 2006 then D_DAT.FISCAL_YEAR
        when 2007 then D_DAT.FISCAL_YEAR
        when 2008 then D_DAT.FISCAL_YEAR
        when 2009 then D_DAT.FISCAL_YEAR
        when 2010 then D_DAT.FISCAL_YEAR
        when 2011 then D_DAT.FISCAL_YEAR
        when 2012 then D_DAT.FISCAL_YEAR
        when 2013 then D_DAT.FISCAL_YEAR
        when 2014 then D_DAT.FISCAL_YEAR
        else 2005 end
, from_unixtime(unix_timestamp())
, from_unixtime(unix_timestamp())


FROM  PS_COMPENSATION C
      LEFT JOIN STG_HCM_MAXEFFDT_XLAT X1 ON X1.FIELDNAME = 'CMP_SRC_IND' AND X1.FIELDVALUE = C.CMP_SRC_IND
      LEFT JOIN DIM_DATE D_DAT ON D_DAT.date_id = C.EFFDT
WHERE
  c.comp_ratecd != 'UNITS'
  AND c.emplid not like 'DUP%'

order by c.EMPLID, c.EMPL_RCD, c.EFFDT, c.EFFSEQ;

CREATE TABLE TMP_ERROR_TABLE AS 
SELECT distinct * FROM STG_HCM_PRE_COMP_SPARK_TEMP c
WHERE c.DW_COMP_LN_CNT > 1;

CREATE TABLE STG_HCM_PRE_COMP_SPARK
AS
SELECT distinct JOB_DAYSEQ_KEY  
, DW_COMP_LN_CNT
, EMPLID        
, EMPL_RCD      
, EFFDT         
, EFFSEQ       
, COMP_EFFSEQ   
, COMP_RATECD   
, COMP_RATECD_DESCR 
, COMP_RATECD2
, COMP_RATECD_DESCR2   
, COMP_RATE_POINTS
, COMPRATE 
, UNITS
, COMP_PCT      
, COMP_FREQUENCY
, COMP_FREQUENCY_DESCR  
, CURRENCY_CD   
, MANUAL_SW     
, CONVERT_COMPRT
, RATE_CODE_GROUP 
, RATE_CODE_GROUP_DESCR 
, CHANGE_AMT    
, CHANGE_PCT    
, CHANGE_PTS    
, FTE_INDICATOR  
, CMP_SRC_IND   
, CMP_SRC_IND_DESCR   
, DW_JOB_ID         
, DW_JOBSEQ         
, DW_START_DATE     
, DW_END_DATE         
, DW_CURRENT_IND
, DW_SOURCE_DB     
, DW_CF_YR
, DW_CREATED_EW_DTTM
, DW_LASTUPD_EW_DTTM
from TMP_ERROR_TABLE e,
(SELECT concat(job_dayseq_key, max(e_ed.dw_end_date)) as edate
  FROM TMP_ERROR_TABLE e_ed
  GROUP BY job_dayseq_key) num2
WHERE concat(e.job_dayseq_key, e.dw_end_date) = num2.edate;

DROP TABLE STG_HCM_PRE_COMP_SPARK_TEMP;
DROP TABLE TMP_ERROR_TABLE;