-- Create STG_HCM_PRE_PER_ORG Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 2/21/2017

set hive.execution.engine=tez;

DROP TABLE STG_HCM_PRE_PER_ORG;

CREATE TABLE STG_HCM_PRE_PER_ORG (
`EMPLID`                String
, `EMPL_RCD`            DECIMAL(38,0)
, `ORG_INSTANCE_ERN`    DECIMAL(38,0)
, `ORIG_HIRE_DT`        TIMESTAMP 
, `PER_ORG`             String
, `BENEFIT_RCD_NBR`     DECIMAL(38,0)
, `CMPNY_SENIORITY_DT`  TIMESTAMP 
, `SERVICE_DT`          TIMESTAMP 
, `LAST_INCREASE_DT`    TIMESTAMP 
, `BUSINESS_TITLE`      String
) stored as orc;

--------------
--We can then populate the table from:
-- ps_psxlatitem
--------------

INSERT INTO STG_HCM_PRE_PER_ORG
(
   emplid
 , empl_rcd
 , org_instance_ern
 , orig_hire_dt 
 , per_org 
 , benefit_rcd_nbr 
 , cmpny_seniority_dt 
 , service_dt 
 , last_increase_dt 
 , business_title 
)
SELECT
   B.EMPLID
 , B.EMPL_RCD
 , B.ORG_INSTANCE_ERN
 , PI.ORIG_HIRE_DT 
 , B.PER_ORG 
 , B.BENEFIT_RCD_NBR 
 , B.CMPNY_SENIORITY_DT 
 , B.SERVICE_DT 
 , B.LAST_INCREASE_DT 
 , B.BUSINESS_TITLE 

    FROM  PS_PER_ORG_ASGN B
        , PS_PER_ORG_INST PI 
 WHERE B.PER_ORG = 'EMP'
   AND PI.EMPLID = B.EMPLID 
   AND PI.ORG_INSTANCE_ERN = B.ORG_INSTANCE_ERN 
       and length(trim(translate(b.emplid,'0123456789',' '))) = 0;