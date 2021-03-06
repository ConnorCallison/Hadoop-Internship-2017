-- Create DIM_HCM_POSITION Table in Hive
-- Author: Connor Callison (ccallison@humboldt.edu)
-- Date Last Modified: 2/28/2017

DROP TABLE IF EXISTS DIM_HCM_POSITION_SPARK;

CREATE TABLE DIM_HCM_POSITION_SPARK (
  `POSITION_KEY`          string
, `POSITION_NBR`	        STRING
, `POSITION_DESCR`        STRING
, `POSITION_DESCRSHORT`   STRING
, `POSITION_BUDGETED`     STRING
, `MAX_HEAD_COUNT`	      Int 
, `ACTIVE_JOB_COUNT`     DECIMAL(38,0) 
, `FTE`	                  Float
);

INSERT INTO DIM_HCM_POSITION_SPARK
SELECT
  p.POSITION_NBR
, p.POSITION_NBR	   
, p.DESCR	      
, p.DESCRSHORT	  
, p.BUDGETED_POSN	   
, p.MAX_HEAD_COUNT
, p.active_job_count
, p.FTE

FROM STG_HCM_DIM_POSITION_SPARK p;
