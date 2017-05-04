
-- This script can be used to calculate the statistics for all of the tables 
-- in our hive database. These statistics are used by the hive cbo (cost based optimzation)
-- to hopefully generate map / reduce jobs that will be faster than without cbo on.
-- From my experience the cbo jobs arent always faster and can sometimes break things. Use but watch carefully.

-- Compute meta statistics for all of the tbales.
ANALYZE TABLE dim_date COMPUTE STATISTICS;
ANALYZE TABLE dim_hcm_position COMPUTE STATISTICS;
ANALYZE TABLE ps_action_tbl COMPUTE STATISTICS;
ANALYZE TABLE ps_actn_reason_tbl COMPUTE STATISTICS;
ANALYZE TABLE ps_compensation COMPUTE STATISTICS;
ANALYZE TABLE ps_csu_job COMPUTE STATISTICS;
ANALYZE TABLE ps_job COMPUTE STATISTICS;
ANALYZE TABLE ps_per_org_asgn COMPUTE STATISTICS;
ANALYZE TABLE ps_per_org_inst COMPUTE STATISTICS;
ANALYZE TABLE ps_position_data COMPUTE STATISTICS;
ANALYZE TABLE psxlatitem COMPUTE STATISTICS;
ANALYZE TABLE stg_hcm_maxeffdt_action COMPUTE STATISTICS;
ANALYZE TABLE stg_hcm_maxeffdt_actrsn COMPUTE STATISTICS;
ANALYZE TABLE stg_hcm_maxeffdt_position COMPUTE STATISTICS;
ANALYZE TABLE stg_hcm_maxeffdt_xlat COMPUTE STATISTICS;
ANALYZE TABLE stg_hcm_pre_comp COMPUTE STATISTICS;
ANALYZE TABLE stg_hcm_pre_per_org COMPUTE STATISTICS;

-- Compute detailed statistics on each column.
ANALYZE TABLE dim_date COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE dim_hcm_position COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_action_tbl COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_actn_reason_tbl COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_compensation COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_csu_job COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_job COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_per_org_asgn COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_per_org_inst COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE ps_position_data COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE psxlatitem COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE stg_hcm_maxeffdt_action COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE stg_hcm_maxeffdt_actrsn COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE stg_hcm_maxeffdt_position COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE stg_hcm_maxeffdt_xlat COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE stg_hcm_pre_comp COMPUTE STATISTICS for COLUMNS;
ANALYZE TABLE stg_hcm_pre_per_org COMPUTE STATISTICS for COLUMNS;
