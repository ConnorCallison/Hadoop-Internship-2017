-- Create STG_HCM_FACT_JOB Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 2/28/2017

set hive.execution.engine=tez;

DROP TABLE STG_HCM_FACT_JOB;

CREATE TABLE STG_HCM_FACT_JOB (
  `JOB_DAYSEQ_KEY`        DECIMAL(38,0)     
, `JOB_DAY_KEY`           DECIMAL(38,0)     
, `EMPLOYEE_DAY_KEY`      DECIMAL(38,0)     
, `JOB_KEY`               DECIMAL(38,0)     
, `CONTRACT_DAY_KEY`      DECIMAL(38,0)  
, `BUSINESS_UNIT`           String  
, `SETID_DEPT`          String  
, `SETID_JOBCODE`           String  
, `DEPTID`          String
, `EMPLID`          String  
, `EMPL_RCD`            DECIMAL(38,0)   
, `DW_JOB_ID`             String        
, `EFFDT`                   TIMESTAMP    
, `EFFSEQ`          DECIMAL(38,0)   
, `DW_JOBSEQ`             DECIMAL(38)       
, `DW_JOBDAY_COUNT`       DECIMAL(38)       
, `DW_POSITION_COUNT`     DECIMAL(38)       
, `DW_START_DATE`         TIMESTAMP      
, `DW_END_DATE`           TIMESTAMP      
, `JOBCODE`         String  
, `POSITION_NBR`            String  
, `CONTRACT_NUM`            String  
, `EMPL_STATUS`         String  
, `EMPL_STATUS_DESCR`     String    
, `HR_STATUS`             String        
, `HR_STATUS_DESCR`       String
, `CSU_PAYROLL_STATUS`  String  
, `CSU_PAYROLL_STATUS_DESCR`    String  
, `ACTION`          String  
, `ACTION_DESCR`            String  
, `ACTION_DT`           TIMESTAMP    
, `ACTION_REASON`           String  
, `ACTION_REASON_DESCR` String  
, `GRADE`                   String  
, `GRADE_ENTRY_DT`  TIMESTAMP    
, `CSU_PROB_CD`         String  
, `CSU_PROB_CD_DESCR`     String
, `CSU_PROB_DT`         TIMESTAMP    
, `EMPL_CLASS`          String  
, `REG_TEMP`            String  
, `FULL_PART_TIME`  String  
, `FLSA_STATUS`         String  
, `PAYGROUP`            String  
, `EMPL_TYPE`           String  
, `EEO_CLASS`           String  
, `SAL_ADMIN_PLAN`  String  
, `JOB_INDICATOR`           String  
, `UNION_CD`        String  
, `CSU_UNIT`            String  
, `CSU_PRIMARY_FUND`    String  
, `CSU_SCO_AGENCY`  String  
, `CSU_SCO_SERIAL`  String  
, `STD_HOURS`           FLOAT   
, `STD_HRS_FREQUENCY`   String  
, `REPORTS_TO`          String  
, `SUPERVISOR_ID`           String
, `FTE`                 FLOAT   
, `JOB_COMP_FREQUENCY`  String  
, `JOB_COMPRATE`    FLOAT   
, `JOB_CALC_COMPRATE`   FLOAT   
, `JOB_CHANGE_AMT`  FLOAT   
, `JOB_CHANGE_PCT`  FLOAT   
, `JOB_ANNUAL_RT`   FLOAT   
, `JOB_MONTHLY_RT`  FLOAT   
, `JOB_DAILY_RT`    FLOAT   
, `JOB_HOURLY_RT`   FLOAT   
, `COMP_COMP_RATECD`    String
, `COMP_COMP_RATECD_DESCR`      String
, `COMP_COMP_RATECD2`       String
, `COMP_COMP_RATECD_DESCR2`     String 
, `COMP_COMP_RATE_POINTS`   DECIMAL(38,0)
, `COMP_COMPRATE`       FLOAT
, `COMP_UNITS`          FLOAT
, `COMP_COMP_PCT`       FLOAT
, `COMP_COMP_FREQUENCY` String
, `COMP_COMP_FREQUENCY_DESCR`   String
, `COMP_CURRENCY_CD`    String
, `COMP_MANUAL_SW`      String
, `COMP_CONVERT_COMPRT` FLOAT
, `COMP_RATE_CODE_GROUP`    String
, `COMP_RATE_CODE_GROUP_DESCR`  String 
, `COMP_CHANGE_AMT`     FLOAT
, `COMP_CHANGE_PCT`     FLOAT
, `COMP_CHANGE_PTS`     FLOAT
, `COMP_FTE_INDICATOR`      String
, `COMP_CMP_SRC_IND`    String
, `COMP_CMP_SRC_IND_DESCR`      String 
, `HIRE_DT`         TIMESTAMP    
, `EXPECTED_RETURN_DT`  TIMESTAMP    
, `EXPECTED_END_DATE`     TIMESTAMP      
, `TERMINATION_DT`  TIMESTAMP    
, `LAST_DATE_WORKED`    TIMESTAMP    
, `JOB_ENTRY_DT`           TIMESTAMP    
, `DEPT_ENTRY_DT`           TIMESTAMP    
, `POSITION_ENTRY_DT`   TIMESTAMP    
, `ENTRY_DATE`          TIMESTAMP    
, `ORIG_HIRE_DT`          TIMESTAMP
, `ASGN_START_DT`         TIMESTAMP
, `LST_ASGN_START_DT`     TIMESTAMP
, `ASGN_END_DT`           TIMESTAMP
, `CSU_ANNI_CD`         String  
, `CSU_ANNI_MONTH`  String  
, `CSU_ANNI_YEAR`           String  
, `CSU_RED_CIRCLE_DT`   TIMESTAMP    
, `CSU_SSI_COUNTER` DECIMAL(38,0)   
, `CSU_LEGAL_REF`           String  
, `CSU_FERP_ELIG_END`   TIMESTAMP    
, `CSU_APPT_DUR`            String  
, `CSU_DIP_ELIG_MONTH`  String  
, `CSU_DIP_ELIG_YEAR`   String  
, `CSU_SAB_ELIG_MONTH`  String  
, `CSU_SAB_ELIG_YEAR`   String  
, `CSU_RET_CD`          String  
, `CSU_GRAND_LEC_CD`    String  
, `CSU_PPT_RUN_DT`  TIMESTAMP    
, `CSU_FRACT_NUM`   INT
, `CSU_FRACT_DENOM` INT
, `CSU_ITEM_215`          String
, `CSU_FURLOUGH_957`    String  
, `CSU_FURLOUGH_815`    Float
, `CSU_FURLOUGH_306`    Float 
, `CSU_SSI_START_DT`    TIMESTAMP    
, `CSU_FMI_AWARD`           Float
, `JOBJR_FP_COMPRATE`   Float
, `JOBJR_FP_FOR_PROM_DT`    TIMESTAMP
, `JED_ACCT_CD`         String  
, `JED_DIST_PCT`            String
, `DW_CURRENT_IND`        String        
, `DW_SOURCE_DB`          String    
, `DW_CF_YR`              INT
, `DW_CREATED_EW_DTTM`    TIMESTAMP      
, `DW_LASTUPD_EW_DTTM`    TIMESTAMP
, `DW_CUR_JOBCODE_START_DATE` TIMESTAMP
, `DW_CUR_POSITION_START_DATE` TIMESTAMP
)
CLUSTERED BY (JOB_DAYSEQ_KEY) INTO 3 BUCKETS
STORED AS orc TBLPROPERTIES('transactional'='true');

--------------
--We can then populate the table from:
-- PS_JOB C
-- STG_HCM_MAXEFFDT_ACTION
-- STG_HCM_MAXEFFDT_ACTRSN
-- STG_HCM_PRE_PER_ORG
-- PS_CSU_JOB
-- STG_HCM_PRE_COMP
-- DIM_DATE
-- STG_HCM_MAXEFFDT_XLAT
--------------

INSERT INTO STG_HCM_FACT_JOB
SELECT
  CAST(CONCAT(date_format(c.effdt,'yyyyMMdd'), lpad(cast(cast(c.effseq as Int) as string), 4, '0'), C.EMPLID, lpad(cast(cast(c.EMPL_RCD as Int) as string), 4, '0')) as DECIMAL(38,0))
, CAST(CONCAT(date_format(c.effdt,'yyyyMMdd'), C.EMPLID, lpad(cast(cast(c.EMPL_RCD as Int) as string), 4, '0')) as DECIMAL(38,0))
, CAST(CONCAT(date_format(c.effdt,'yyyyMMdd'), C.EMPLID) as DECIMAL(38,0))
, cast(CONCAT('9', c.EMPLID, lpad(cast(cast(c.EMPL_RCD as Int) as string), 4, '0')) as DECIMAL(38,0))
, 0
, C.BUSINESS_UNIT 
, C.SETID_DEPT 
, C.SETID_JOBCODE 
, C.DEPTID 
, C.EMPLID 
, C.EMPL_RCD 
, CONCAT(C.EMPLID, '-', lpad(cast(cast(c.EMPL_RCD as int) as string), 4, '0'))
, C.EFFDT 
, C.EFFSEQ 
, dense_rank() over (partition by c.emplid,c.empl_rcd order by c.effdt desc,c.effseq desc) as dw_jobseq
, count(*) over (partition by c.emplid,c.empl_rcd,c.effdt) as DW_JOBDAY_COUNT
, 0
, C.EFFDT as dw_start_date
, LEAD(C.EFFDT,1,TO_DATE('2099-6-30')) OVER (PARTITION BY C.EMPLID, C.EMPL_RCD ORDER BY C.effdt, C.effseq) as dw_end_date
, C.JOBCODE 
, C.POSITION_NBR 
, C.CONTRACT_NUM
, C.EMPL_STATUS 
, NVL(X1.XLATLONGNAME,'-')
, C.HR_STATUS
, NVL(X2.XLATLONGNAME,'-')
, Z.CSU_PAYROLL_STATUS 
, NVL(X3.XLATLONGNAME,'-')
, C.ACTION 
, NVL(A.ACTION_DESCR,'-') 
, C.ACTION_DT 
, C.ACTION_REASON 
, NVL(AR.DESCR,'-') 
, C.GRADE 
, C.GRADE_ENTRY_DT 
, Z.CSU_PROB_CD 
, NVL(X4.XLATLONGNAME,'-')
, Z.CSU_PROB_DT 
, C.EMPL_CLASS
, C.REG_TEMP 
, C.FULL_PART_TIME 
, C.FLSA_STATUS 
, C.PAYGROUP 
, C.EMPL_TYPE 
, C.EEO_CLASS 
, C.SAL_ADMIN_PLAN 
, C.JOB_INDICATOR 
, if(C.UNION_CD=' ', '-', C.UNION_CD) dec
, Z.CSU_UNIT 
, Z.CSU_PRIMARY_FUND 
, Z.CSU_SCO_AGENCY 
, Z.CSU_SCO_SERIAL 
, C.STD_HOURS 
, C.STD_HRS_FREQUENCY 
, C.REPORTS_TO 
, C.SUPERVISOR_ID 
, C.FTE 
, c.COMP_FREQUENCY
, c.COMPRATE        
, 0 
, c.CHANGE_AMT
, c.CHANGE_PCT      
, c.ANNUAL_RT   
, c.MONTHLY_RT  
, c.DAILY_RT    
, c.HOURLY_RT   
, b.COMP_RATECD   
, b.COMP_RATECD_DESCR   
, NVL(b.COMP_RATECD2,'-')
, NVL(b.COMP_RATECD_DESCR2, '-')
, b.COMP_RATE_POINTS
, b.COMPRATE   
, NVL(TRIM(CAST(b.UNITS as String)),0)     
, b.COMP_PCT     
, b.COMP_FREQUENCY  
, b.COMP_FREQUENCY_DESCR    
, b.CURRENCY_CD  
, b.MANUAL_SW    
, b.CONVERT_COMPRT
, b.RATE_CODE_GROUP 
, b.RATE_CODE_GROUP_DESCR
, b.CHANGE_AMT   
, b.CHANGE_PCT  
, b.CHANGE_PTS    
, b.FTE_INDICATOR 
, b.CMP_SRC_IND   
, b.CMP_SRC_IND_DESCR   
, C.HIRE_DT 
, C.EXPECTED_RETURN_DT 
, C.EXPECTED_END_DATE
, C.TERMINATION_DT 
, C.LAST_DATE_WORKED 
, C.JOB_ENTRY_DT 
, C.DEPT_ENTRY_DT 
, C.POSITION_ENTRY_DT 
, C.ENTRY_DATE 
, PO.ORIG_HIRE_DT
, C.ASGN_START_DT        
, C.LST_ASGN_START_DT    
, C.ASGN_END_DT          
, Z.CSU_ANNI_CD 
, Z.CSU_ANNI_MONTH 
, Z.CSU_ANNI_YEAR 
, Z.CSU_RED_CIRCLE_DT 
, Z.CSU_SSI_COUNTER
, Z.CSU_LEGAL_REF 
, Z.CSU_FERP_ELIG_END 
, Z.CSU_APPT_DUR 
, Z.CSU_DIP_ELIG_MONTH 
, Z.CSU_DIP_ELIG_YEAR 
, Z.CSU_SAB_ELIG_MONTH 
, Z.CSU_SAB_ELIG_YEAR 
, Z.CSU_RET_CD 
, Z.CSU_GRAND_LEC_CD
, Z.CSU_PPT_RUN_DT 
, Z.CSU_FRACT_NUM 
, Z.CSU_FRACT_DENOM
, Z.CSU_ITEM_215 
, Z.CSU_FURLOUGH_957
, Z.CSU_FURLOUGH_815
, Z.CSU_FURLOUGH_306
, Z.CSU_SSI_START_DT
, Z.CSU_FMI_AWARD
, Z.CSU_FMI_AWARD
, z.CSU_SSI_START_DT 
, '-'
, '-'
, '-'
, 'DW_COMMON.DB_CMS_HR'
,  CASE WHEN D_DAT.FISCAL_YEAR < 2005 THEN 2005
         ELSE D_DAT.FISCAL_YEAR END
, from_unixtime(unix_timestamp())
, from_unixtime(unix_timestamp())
, from_unixtime(unix_timestamp())
, from_unixtime(unix_timestamp())


FROM 
     PS_JOB C 
     JOIN PS_CSU_JOB Z ON Z.EMPLID = C.EMPLID AND Z.EMPL_RCD = C.EMPL_RCD AND Z.EFFDT = C.EFFDT AND Z.EFFSEQ = C.EFFSEQ
     LEFT JOIN STG_HCM_MAXEFFDT_ACTION A ON A.ACTION = C.ACTION
     LEFT JOIN STG_HCM_MAXEFFDT_ACTRSN AR ON AR.ACTION = C.ACTION AND AR.ACTION_REASON = C.ACTION_REASON
     LEFT JOIN STG_HCM_PRE_PER_ORG PO ON PO.EMPLID = C.EMPLID
     LEFT JOIN STG_HCM_PRE_COMP B ON B.EMPLID = C.EMPLID AND B.EMPL_RCD = C.EMPL_RCD AND B.EFFDT = C.EFFDT AND B.EFFSEQ = C.EFFSEQ
     LEFT JOIN DIM_DATE D_DAT ON c.effdt = d_dat.date_id
     LEFT JOIN STG_HCM_MAXEFFDT_XLAT X1 ON X1.FIELDNAME = 'EMPL_STATUS' AND X1.FIELDVALUE = C.EMPL_STATUS
     LEFT JOIN STG_HCM_MAXEFFDT_XLAT X2 ON X2.FIELDNAME = 'HR_STATUS' AND X2.FIELDVALUE = C.HR_STATUS
     LEFT JOIN STG_HCM_MAXEFFDT_XLAT X3 ON X3.FIELDNAME = 'CSU_PAYROLL_STATUS' AND X3.FIELDVALUE = Z.CSU_PAYROLL_STATUS
     LEFT JOIN STG_HCM_MAXEFFDT_XLAT X4 ON X4.FIELDNAME = 'CSU_PROB_CD' AND X4.FIELDVALUE = Z.CSU_PROB_CD
WHERE 
    PO.EMPL_RCD = C.EMPL_RCD

order by EMPLID , EMPL_RCD , EFFDT , EFFSEQ;

update STG_HCM_FACT_JOB set dw_current_ind = 'Y'
    where 
    dw_jobseq in 
      (
        select min(a.dw_jobseq)
        from STG_HCM_FACT_JOB a, STG_HCM_FACT_JOB m
        where 
        a.emplid = m.emplid
        and a.empl_rcd = m.empl_rcd
      );