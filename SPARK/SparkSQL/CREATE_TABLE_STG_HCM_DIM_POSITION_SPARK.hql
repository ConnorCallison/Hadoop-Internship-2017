-- Create STG_HCM_DIM_POSITION Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 12/16/2017

DROP TABLE STG_HCM_DIM_POSITION;
CREATE TABLE STG_HCM_DIM_POSITION (
`POSITION_NBR`            String
, `EFFDT`                    TIMESTAMP
, `EFF_STATUS`            String
, `DESCR`            String
, `DESCRSHORT`            String
, `ACTION`            String
, `ACTION_DESCR`            String
, `ACTION_REASON`            String
, `ACTION_REASON_DESCR`    String
, `ACTION_DT`            DATE
, `BUSINESS_UNIT`            String
, `DEPTID`            String
, `JOBCODE`            String
, `POSN_STATUS`            String
, `STATUS_DT`            DATE
, `BUDGETED_POSN`            String
, `CONFIDENTIAL_POSN`    String
, `MAX_HEAD_COUNT`    int
, `REPORTS_TO`            String
, `REPORTS_TO_POS_CNT`    int
, `REPORTS_TO_EMPLID`     String
, `REPORTS_TO_NAME`       String
, `STD_HOURS`            FLOAT
, `STD_HRS_FREQUENCY`    String
, `UNION_CD`            String
, `REG_TEMP`            String
, `FULL_PART_TIME`    String
, `FLSA_STATUS`            String
, `FTE`                    FLOAT
, `POSITION_POOL_ID`    String
, `ADDS_TO_FTE_ACTUAL`    String
, `SAL_ADMIN_PLAN`    String
, `GRADE`                    String
, `ACTIVE_JOB_COUNT`      int
);

--------------
--We can then populate the table from:
-- stg_hcm_maxeffdt_action
-- stg_hcm_maxeffdt_actrsn
-- stg_hcm_maxeffdt_position
--------------

INSERT INTO STG_HCM_DIM_POSITION
SELECT
  p.POSITION_NBR     
, p.EFFDT       
, p.EFF_STATUS  
, p.DESCR       
, p.DESCRSHORT    
, p.ACTION
, NVL(A.ACTION_DESCR,''-'') 
, p.ACTION_REASON  
, NVL(AR.DESCR,''-'') 
, p.ACTION_DT 
, p.BUSINESS_UNIT    
, p.DEPTID
, p.JOBCODE 
, p.POSN_STATUS 
, p.STATUS_DT  
, p.BUDGETED_POSN    
, p.CONFIDENTIAL_POSN
, p.MAX_HEAD_COUNT
, p.REPORTS_TO  
, cast (null as int)
, ''-''
, ''-''
, p.STD_HOURS 
, p.STD_HRS_FREQUENCY
, p.UNION_CD
, p.REG_TEMP
, p.FULL_PART_TIME
, p.FLSA_STATUS
, p.FTE
, p.POSITION_POOL_ID
, p.ADDS_TO_FTE_ACTUAL
, p.SAL_ADMIN_PLAN
, p.GRADE
, NVL(PC.DW_POSITION_COUNT,0) 

FROM
       STG_HCM_MAXEFFDT_POSITION  P
       LEFT JOIN STG_HCM_MAXEFFDT_ACTION A ON A.ACTION  = P.ACTION
       LEFT JOIN STG_HCM_MAXEFFDT_ACTRSN AR ON AR.ACTION = P.ACTION AND AR.ACTION_REASON = P.ACTION_REASON
       LEFT JOIN ( 
                  select distinct dW_position_count, position_nbr 
                  from stg_hcm_dim_job j
                  where j.dw_current_ind = 'Y'
                  and j.business_unit = 'HMCMP'
                  and j.hr_status = 'A'
                 ) pc ON pc.position_nbr = p.position_nbr;

