-- Create STG_HCM89_DIM_JOB Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- TIMESTAMP Last Modified: 2/23/2017

set hive.execution.engine=tez;

DROP TABLE STG_HCM_DIM_JOB;

CREATE TABLE STG_HCM_DIM_JOB
(
  `JOB_DAYSEQ_KEY`           DECIMAL(38,0)      
, `JOB_DAY_KEY`              DECIMAL(38,0)      
, `JOB_KEY`                  DECIMAL(38,0)      
, `BUSINESS_UNIT`            String
, `DEPTID`                   String  
, `EMPLID`                   String  
, `EMPL_RCD`                 DECIMAL(38,0)   
, `EFFDT`                    TIMESTAMP   
, `EFFSEQ`                   DECIMAL(38,0)   
, `DW_JOB_ID`                String     
, `POSITION_NBR`             String  
, `CONTRACT_NUM`             String  
, `EMPL_STATUS`              String  
, `EMPL_STATUS_DESCR`        String     
, `HR_STATUS`                String     
, `HR_STATUS_DESCR`          String     
, `CSU_PAYROLL_STATUS`       String  
, `CSU_PAYROLL_STATUS_DESCR` String  
, `ACTION`                   String  
, `ACTION_DESCR`             String  
, `ACTION_REASON`            String  
, `ACTION_REASON_DESCR`      String  
, `CSU_PROB_CD`              String  
, `CSU_PROB_CD_DESCR`        String
, `EMPL_CLASS`               String
, `EMPL_CLASS_DESCR`         String
, `REG_TEMP`                 String
, `REG_TEMP_DESCR`           String 
, `FULL_PART_TIME`           String
, `FULL_PART_TIME_DESCR`     String
, `FLSA_STATUS`              String
, `FLSA_STATUS_DESCR`        String 
, `PAYGROUP`                 String
, `PAYGROUP_DESCR`           String 
, `EMPL_TYPE`                String
, `EMPL_TYPE_DESCR`          String 
, `EEO_CLASS`                String
, `EEO_CLASS_DESCR`          String 
, `JOB_INDICATOR`            String
, `JOB_INDICATOR_DESCR`      String
, `UNION_CD`                 String
, `UNION_CD_DESCR`           String
, `CSU_UNIT`                 String
, `CSU_UNIT_DESCR`           String
, `CSU_PRIMARY_FUND`         String
, `CSU_PRIMARY_FUND_DESCR`   String
, `CSU_SCO_AGENCY`           String
, `CSU_SCO_AGENCY_DESCR`     String
, `CSU_SCO_SERIAL`           String
, `STD_HOURS`                FLOAT
, `STD_HRS_FREQUENCY`        String
, `STD_HRS_FREQUENCY_DESCR`  String
, `REPORTS_TO`               String
, `REPORTS_TO_DESCR`         String
, `SUPERVISOR_ID`            String
, JOB_COMP_FREQUENCY         String  
, JOB_COMPRATE               FLOAT   
, JOB_CALC_COMPRATE          FLOAT   
, JOB_CHANGE_AMT             FLOAT   
, JOB_CHANGE_PCT             FLOAT   
, JOB_ANNUAL_RT              FLOAT   
, JOB_MONTHLY_RT             FLOAT   
, JOB_DAILY_RT               FLOAT   
, JOB_HOURLY_RT              FLOAT   
, COMP_COMP_RATECD           String
, COMP_COMP_RATECD_DESCR     String 
, COMP_COMPRATE              FLOAT
, COMP_COMP_FREQUENCY        String
, COMP_COMP_FREQUENCY_DESCR  String 
, COMP_MANUAL_SW             String
, COMP_CONVERT_COMPRT        Float
, COMP_CHANGE_AMT            Float
, COMP_CHANGE_PCT            Float
, COMP_FTE_INDICATOR         String
, COMP_CMP_SRC_IND           String
, COMP_CMP_SRC_IND_DESCR     String 
, CSU_SSI_COUNTER            DECIMAL(38,0)   
, CSU_FERP_ELIG_END          TIMESTAMP   
, CSU_DIP_ELIG_MONTH         String  
, CSU_DIP_ELIG_YEAR          String  
, CSU_SAB_ELIG_MONTH         String  
, CSU_SAB_ELIG_YEAR          String  
, CSU_FRACT_NUM              Int
, CSU_FRACT_DENOM            Int
, CSU_ITEM_215               String
, JOBJR_FP_COMPRATE          Float
, JOBJR_FP_FOR_PROM_DT       TIMESTAMP
, HIRE_DT                    TIMESTAMP   
, EXPECTED_RETURN_DT         TIMESTAMP   
, EXPECTED_END_DATE          TIMESTAMP       
, TERMINATION_DT             TIMESTAMP   
, LAST_DATE_WORKED           TIMESTAMP   
, JOB_ENTRY_DT               TIMESTAMP   
, DEPT_ENTRY_DT              TIMESTAMP   
, POSITION_ENTRY_DT          TIMESTAMP   
, ENTRY_DATE                 TIMESTAMP   
, GRADE_ENTRY_DT             TIMESTAMP   
, ORIG_HIRE_DT               TIMESTAMP 
, ACTION_DT                  TIMESTAMP   
, CSU_PROB_DT                TIMESTAMP   
, LST_ASGN_START_DT          TIMESTAMP 
, JED_ACCT_CD                String  
, JED_DIST_PCT               String  
, FTE                        Float
, DW_CURRENT_IND             String      
, DW_JOBSEQ                  DECIMAL(38,0)       
, DW_JOBDAY_COUNT            DECIMAL(38,0)       
, DW_POSITION_COUNT          DECIMAL(38,0)       
, DW_START_DATE              TIMESTAMP       
, DW_END_DATE                TIMESTAMP       
, DW_SOURCE_DB               String
, COMP_RATECD2               String
, COMP_RATECD_DESCR2         String
, COMP_UNITS                 Float
, DW_JOBCODE_START_DT        TIMESTAMP
, DW_POSITION_START_DT       TIMESTAMP
) stored as orc;


--------------
--We can then populate the table from:
-- STG_HCM_FACT_JOB c
-- STG_HCM_MAXEFFDT_XLAT
--------------

INSERT INTO STG_HCM_DIM_JOB
(
job_dayseq_key        
, job_day_key           
, job_key               
, business_unit
, deptid      
, emplid    
, empl_rcd      
, effdt          
, effseq     
, dw_job_id         
, position_nbr
, contract_num
, empl_status       
, empl_status_descr         
, hr_status             
, hr_status_descr           
, csu_payroll_status    
, csu_payroll_status_descr  
, action        
, action_descr      
, action_reason     
, action_reason_descr       
, csu_prob_cd           
, csu_prob_cd_descr  
, csu_prob_dt       
, empl_class
, empl_class_descr
, reg_temp
, reg_temp_descr
, full_part_time
, full_part_time_descr
, flsa_status
, flsa_status_descr
, paygroup
, empl_type
, empl_type_descr
, eeo_class
, eeo_class_descr
, job_indicator
, job_indicator_descr
, union_cd
, union_cd_descr
, csu_unit
, csu_unit_descr
, csu_primary_fund
, csu_primary_fund_descr
, csu_sco_agency
, csu_sco_serial
, std_hours
, std_hrs_frequency
, reports_to
, supervisor_id
, job_comp_frequency
, job_comprate      
, job_calc_comprate 
, job_change_amt
, job_change_pct    
, job_annual_rt     
, job_monthly_rt    
, job_daily_rt  
, job_hourly_rt 
, comp_comp_ratecd  
, comp_comp_ratecd_descr    
, comp_comprate   
, comp_comp_frequency   
, comp_comp_frequency_descr 
, comp_manual_sw   
, comp_convert_comprt
, comp_change_amt   
, comp_change_pct 
, comp_fte_indicator  
, comp_cmp_src_ind   
, comp_cmp_src_ind_descr   
, csu_ssi_counter       
, csu_ferp_elig_end 
, csu_dip_elig_month    
, csu_dip_elig_year 
, csu_sab_elig_month    
, csu_sab_elig_year
, csu_fract_num 
, csu_fract_denom
, csu_item_215 
, jobjr_fp_comprate
, jobjr_fp_for_prom_dt
, hire_dt   
, expected_return_dt        
, expected_end_date         
, termination_dt        
, last_date_worked  
, job_entry_dt     
, dept_entry_dt  
, position_entry_dt 
, entry_date     
, grade_entry_dt    
, orig_hire_dt     
, action_dt     
, lst_asgn_start_dt   
, jed_acct_cd       
, jed_dist_pct     
, fte
, dw_current_ind        
, dw_jobseq             
, dw_jobday_count           
, dw_position_count         
, dw_start_date  
, dw_end_date       
, dw_source_db
, comp_ratecd2
, comp_ratecd_descr2
, comp_units
, dw_jobcode_start_dt
, dw_position_start_dt
)
SELECT 
  c.JOB_DAYSEQ_KEY          
, c.JOB_DAY_KEY         
, c.JOB_KEY         
, c.BUSINESS_UNIT    
, c.DEPTID
, c.EMPLID  
, c.EMPL_RCD        
, c.EFFDT             
, c.EFFSEQ      
, c.DW_JOB_ID           
, c.POSITION_NBR
, c.CONTRACT_NUM
, c.EMPL_STATUS     
, c.EMPL_STATUS_DESCR           
, c.HR_STATUS               
, c.HR_STATUS_DESCR         
, c.CSU_PAYROLL_STATUS  
, c.CSU_PAYROLL_STATUS_DESCR    
, c.ACTION      
, c.ACTION_DESCR        
, c.ACTION_REASON       
, c.ACTION_REASON_DESCR     
, c.CSU_PROB_CD         
, c.CSU_PROB_CD_DESCR  
, c.CSU_PROB_DT     
, c.EMPL_CLASS
, NVL(X1.XLATLONGNAME,'-')
, c.REG_TEMP
, NVL(X2.XLATLONGNAME,'-')
, c.FULL_PART_TIME
, NVL(X3.XLATLONGNAME,'-')
, c.FLSA_STATUS
, NVL(X4.XLATLONGNAME,'-')
, c.PAYGROUP
, c.EMPL_TYPE
, NVL(X6.XLATLONGNAME,'-')
, c.EEO_CLASS
, NVL(X5.XLATLONGNAME,'-')
, c.JOB_INDICATOR
, NVL(X7.XLATLONGNAME,'-')
, c.UNION_CD
, '-'
, c.CSU_UNIT
, '-'
, c.CSU_PRIMARY_FUND
, '-'
, c.CSU_SCO_AGENCY
, c.CSU_SCO_SERIAL
, c.STD_HOURS
, c.STD_HRS_FREQUENCY
, c.REPORTS_TO
, c.SUPERVISOR_ID
, c.JOB_COMP_FREQUENCY
, c.JOB_COMPRATE    
, c.JOB_CALC_COMPRATE   
, c.JOB_CHANGE_AMT
, c.JOB_CHANGE_PCT  
, c.JOB_ANNUAL_RT   
, c.JOB_MONTHLY_RT  
, c.JOB_DAILY_RT  
, c.JOB_HOURLY_RT 
, c.COMP_COMP_RATECD  
, c.COMP_COMP_RATECD_DESCR    
, c.COMP_COMPRATE   
, c.COMP_COMP_FREQUENCY 
, c.COMP_COMP_FREQUENCY_DESCR 
, c.COMP_MANUAL_SW   
, c.COMP_CONVERT_COMPRT
, c.COMP_CHANGE_AMT   
, c.COMP_CHANGE_PCT 
, c.COMP_FTE_INDICATOR  
, c.COMP_CMP_SRC_IND   
, c.COMP_CMP_SRC_IND_DESCR   
, c.CSU_SSI_COUNTER     
, c.CSU_FERP_ELIG_END   
, c.CSU_DIP_ELIG_MONTH  
, c.CSU_DIP_ELIG_YEAR   
, c.CSU_SAB_ELIG_MONTH  
, c.CSU_SAB_ELIG_YEAR
, c.CSU_FRACT_NUM 
, c.CSU_FRACT_DENOM
, c.CSU_ITEM_215 
, c.JOBJR_FP_COMPRATE
, c.JOBJR_FP_FOR_PROM_DT
, c.HIRE_DT 
, c.EXPECTED_RETURN_DT      
, c.EXPECTED_END_DATE       
, c.TERMINATION_DT      
, c.LAST_DATE_WORKED    
, c.JOB_ENTRY_DT       
, c.DEPT_ENTRY_DT    
, c.POSITION_ENTRY_DT   
, c.ENTRY_DATE   
, c.GRADE_ENTRY_DT  
, c.ORIG_HIRE_DT     
, c.ACTION_DT       
, c.LST_ASGN_START_DT  
, c.JED_ACCT_CD     
, c.JED_DIST_PCT    
, c.FTE
, c.DW_CURRENT_IND          
, c.DW_JOBSEQ           
, c.DW_JOBDAY_COUNT         
, count(*) over (partition by c.position_nbr,c.dw_current_ind,c.hr_status,c.business_unit) as DW_POSITION_COUNT
, c.DW_START_DATE  
, c.DW_END_DATE    
, c.DW_SOURCE_DB
, NVL(c.COMP_COMP_RATECD2,'-')  
, NVL(c.COMP_COMP_RATECD_DESCR2, '-')
, c.COMP_UNITS
, c.dw_cur_jobcode_start_date
, C.DW_CUR_POSITION_START_DATE

FROM
    STG_HCM_FACT_JOB c
    LEFT JOIN STG_HCM_MAXEFFDT_XLAT X1 ON X1.FIELDNAME = 'EMPL_CLASS' AND X1.FIELDVALUE = C.EMPL_CLASS
    LEFT JOIN STG_HCM_MAXEFFDT_XLAT X2 ON X2.FIELDNAME = 'REG_TEMP' AND X2.FIELDVALUE = C.REG_TEMP
    LEFT JOIN STG_HCM_MAXEFFDT_XLAT X3 ON X3.FIELDNAME = 'FULL_PART_TIME' AND X3.FIELDVALUE = C.FULL_PART_TIME
    LEFT JOIN STG_HCM_MAXEFFDT_XLAT X4 ON X4.FIELDNAME = 'FLSA_STATUS' AND X4.FIELDVALUE = C.FLSA_STATUS
    LEFT JOIN STG_HCM_MAXEFFDT_XLAT X5 ON X5.FIELDNAME = 'EEO_CLASS' AND X5.FIELDVALUE = C.EEO_CLASS
    LEFT JOIN STG_HCM_MAXEFFDT_XLAT X6 ON X6.FIELDNAME = 'EMPL_TYPE' AND X6.FIELDVALUE = C.EMPL_TYPE
    LEFT JOIN STG_HCM_MAXEFFDT_XLAT X7 ON X7.FIELDNAME = 'JOB_INDICATOR' AND X7.FIELDVALUE = C.JOB_INDICATOR;
