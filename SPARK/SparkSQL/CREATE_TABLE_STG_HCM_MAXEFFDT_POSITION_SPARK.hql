-- Create STG_HCM_MAXEFFDT_POSITION_SPARK Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 12/16/2017

DROP TABLE IF EXISTS STG_HCM_MAXEFFDT_POSITION_SPARK;

CREATE TABLE STG_HCM_MAXEFFDT_POSITION_SPARK (
`POSITION_NBR`	        STRING,
`EFFDT`	                TIMESTAMP,
`EFF_STATUS`	        STRING,
`DESCR`	                STRING,
`DESCRSHORT`	        STRING,
`ACTION`	        STRING,
`ACTION_REASON`	        STRING,
`ACTION_DT`	        TIMESTAMP,
`BUSINESS_UNIT`	        STRING,
`DEPTID`	        STRING,
`JOBCODE`	        STRING,
`POSN_STATUS`	        STRING,
`STATUS_DT`	        TIMESTAMP,
`BUDGETED_POSN`	        STRING,
`CONFIDENTIAL_POSN`	STRING,
`KEY_POSITION`          STRING,
`JOB_SHARE`	        STRING,
`MAX_HEAD_COUNT`	Int,
`UPDATE_INCUMBENTS`	STRING,
`REPORTS_TO`	        STRING,
`REPORT_DOTTED_LINE`	STRING,
`ORGCODE`	        STRING,
`ORGCODE_FLAG`	        STRING,
`LOCATION`	        STRING,
`MAIL_DROP`	        STRING,
`COUNTRY_CODE`	        STRING,
`PHONE`	                STRING,
`COMPANY`	        STRING,
`STD_HOURS`	        Float,
`STD_HRS_FREQUENCY`	STRING,
`UNION_CD`	        STRING,
`SHIFT`	                STRING,
`REG_TEMP`	        STRING,
`FULL_PART_TIME`	STRING,
`BARG_UNIT`	        STRING,
`SEASONAL`	        STRING,
`TRN_PROGRAM`	        STRING,
`LANGUAGE_SKILL`	STRING,
`MANAGER_LEVEL`	        STRING,
`FLSA_STATUS`	        STRING,
`AVAIL_TELEWORK_POS`	STRING,
`CLASS_INDC`	        STRING,
`ENCUMBER_INDC`	        STRING,
`FTE`	                Float,
`POSITION_POOL_ID`	STRING,
`EG_ACADEMIC_RANK`	STRING,
`EG_GROUP`	        STRING,
`ENCUMB_SAL_OPTN`	STRING,
`ENCUMB_SAL_AMT`	Float,
`HEALTH_CERTIFICATE`	STRING,
`SIGN_AUTHORITY`	STRING,
`ADDS_TO_FTE_ACTUAL`	STRING,
`SAL_ADMIN_PLAN`	STRING,
`GRADE`	                STRING
);

------------------------------------------
--Use the following to insert the data needed to complete this table.
------------------------------------------

INSERT INTO STG_HCM_MAXEFFDT_POSITION_SPARK
SELECT
  p.position_nbr	   
, p.effdt      
, p.eff_status	
, p.descr	      
, p.descrshort 
, p.action
, p.action_reason	 
, p.action_dt	
, p.business_unit	   
, p.deptid
, p.jobcode	
, p.posn_status	
, p.status_dt	 
, p.budgeted_posn	   
, p.confidential_posn
, p.key_position	  
, p.job_share	
, p.max_head_count
, p.update_incumbents
, p.reports_to	
, p.report_dotted_line
, p.orgcode	
, p.orgcode_flag
, p.location	
, p.mail_drop
, p.country_code	
, p.phone	  
, p.company
, p.std_hours	
, p.std_hrs_frequency
, p.union_cd
, p.shift	  
, p.reg_temp
, p.full_part_time
, p.barg_unit	
, p.seasonal
, p.trn_program	
, p.language_skill
, p.manager_level
, p.flsa_status
, p.avail_telework_pos
, p.class_indc
, p.encumber_indc	 
, p.fte
, p.position_pool_id
, p.eg_academic_rank
, p.eg_group
, p.encumb_sal_optn
, p.encumb_sal_amt
, p.health_certificate
, p.sign_authority
, p.adds_to_fte_actual
, p.sal_admin_plan
, p.grade
FROM
  PS_POSITION_DATA  p,
  (SELECT MAX(C_ED.EFFDT) as EFFDT, C_ED.POSITION_NBR
        FROM PS_POSITION_DATA C_ED
        WHERE C_ED.EFFDT <= from_unixtime(unix_timestamp())
        GROUP BY C_ED.POSITION_NBR) maxeffdt
WHERE  
    P.EFFDT = maxeffdt.EFFDT
    AND P.POSITION_NBR = maxeffdt.POSITION_NBR;