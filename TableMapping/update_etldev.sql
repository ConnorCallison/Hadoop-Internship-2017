-- Refresh ETLDEV tables from dev database

truncate table etldev.dim_hcm_position;
truncate table etldev.fact_hcm_lcd;
truncate table etldev.ps_acad_org_tbl;
truncate table etldev.ps_action_tbl;
truncate table etldev.ps_actn_reason_tbl;
truncate table etldev.ps_addresses;
truncate table etldev.ps_adm_appl_prog;
truncate table etldev.ps_bas_partic;
truncate table etldev.ps_citizen_sts_tbl;
truncate table etldev.ps_compensation;
truncate table etldev.ps_country_tbl;
truncate table etldev.ps_csu_cntrct_calc;
truncate table etldev.ps_csu_ethnic_map;
truncate table etldev.ps_csu_job;
truncate table etldev.ps_csu_prior_exp;
truncate table etldev.ps_dept_tbl;
truncate table etldev.ps_divers_ethnic;
truncate table etldev.ps_email_addresses;
truncate table etldev.ps_ethnic_grp_tbl;
truncate table etldev.ps_job;
truncate table etldev.ps_names;
truncate table etldev.ps_per_checklist;
truncate table etldev.ps_per_org_asgn;
truncate table etldev.ps_per_org_inst;
truncate table etldev.ps_pers_data_effdt;
truncate table etldev.ps_pers_nid;
truncate table etldev.ps_person;
truncate table etldev.ps_person_name;
truncate table etldev.ps_personal_data;
truncate table etldev.ps_personal_phone;
truncate table etldev.ps_position_data;
truncate table etldev.ps_school_tbl;
truncate table etldev.ps_stdnt_enrl;
truncate table etldev.ps_tree_node_tbl;
truncate table etldev.pstreeleaf;
truncate table etldev.psxlatitem;
truncate table etldev.STG_HCM_DIM_JOB;
truncate table etldev.STG_HCM_DIM_POSITION;
truncate table etldev.STG_HCM_FACT_JOB;
truncate table etldev.STG_HCM_MAXEFFDT_ACTION;
truncate table etldev.STG_HCM_MAXEFFDT_ACTRSN;
truncate table etldev.STG_HCM_MAXEFFDT_POSITION;
truncate table etldev.STG_HCM_MAXEFFDT_XLAT;
truncate table etldev.STG_HCM_PRE_COMP;
truncate table etldev.STG_HCM_PRE_PER_ORG;

insert into etldev.dim_hcm_position select * from dw.dim_hcm_position;
insert into etldev.fact_hcm_lcd select * from dw.fact_hcm_lcd;
insert into etldev.ps_acad_org_tbl select * from synhr_acad_org_tbl;
insert into etldev.ps_action_tbl select * from synhr_action_tbl;
insert into etldev.ps_actn_reason_tbl select * from synhr_actn_reason_tbl;
insert into etldev.ps_addresses select * from synhr_ps_addresses;
insert into etldev.ps_adm_appl_prog select * from ps_stu.ps_adm_appl_prog;
insert into etldev.ps_bas_partic select * from synhr_bas_partic;
insert into etldev.ps_citizen_sts_tbl select * from synhr_citizen_sts_tbl;
insert into etldev.ps_compensation select * from synhr_compensation;
insert into etldev.ps_country_tbl select * from synhr_country_tbl;
insert into etldev.ps_csu_cntrct_calc select * from synhr_csu_cntrct_calc;
insert into etldev.ps_csu_ethnic_map select * from SYNHR_csu_ethnic_map;
insert into etldev.ps_csu_job select * from synhr_csu_job;
insert into etldev.ps_csu_prior_exp select * from synhr_csu_prior_exp;
insert into etldev.ps_dept_tbl select * from synstu_dept_tbl;
insert into etldev.ps_divers_ethnic select * from synhr_divers_ethnic;
insert into etldev.ps_email_addresses select * from synhr_email_addresses;
insert into etldev.ps_ethnic_grp_tbl select * from synhr_ethnic_grp_tbl;
insert into etldev.ps_job select * from synhr_job;
insert into etldev.ps_names select * from SYNHR_ps_names;
insert into etldev.ps_per_checklist select * from synhr_per_checklist;
insert into etldev.ps_per_org_asgn select * from synhr_per_org_asgn;
insert into etldev.ps_per_org_inst select * from synhr_per_org_inst;
insert into etldev.ps_pers_data_effdt select * from synhr_pers_data_effdt;
insert into etldev.ps_pers_nid select * from synhr_pers_nid;
insert into etldev.ps_person select * from synhr_person;
insert into etldev.ps_person_name select * from synhr_person_name;
insert into etldev.ps_personal_data select * from synhr_personal_data;
insert into etldev.ps_personal_phone select * from synhr_personal_phone;
insert into etldev.ps_position_data select * from synhr_position_data;
insert into etldev.ps_school_tbl select * from synhr_school_tbl;
insert into etldev.ps_stdnt_enrl select * from ps_stu.ps_stdnt_enrl;
insert into etldev.ps_tree_node_tbl select * from syncfs_tree_node_tbl;
insert into etldev.pstreeleaf select * from syncfs_pstreeleaf;
insert into etldev.psxlatitem select * from SYNHR_psxlatitem;
insert into etldev.STG_HCM_DIM_JOB select * from STG_HCM_DIM_JOB;
insert into etldev.STG_HCM_DIM_POSITION select * from STG_HCM_DIM_POSITION;
insert into etldev.STG_HCM_FACT_JOB select * from STG_HCM_FACT_JOB;
insert into etldev.STG_HCM_MAXEFFDT_ACTION select * from STG_HCM_MAXEFFDT_ACTION;
insert into etldev.STG_HCM_MAXEFFDT_ACTRSN select * from STG_HCM_MAXEFFDT_ACTRSN;
insert into etldev.STG_HCM_MAXEFFDT_POSITION select * from STG_HCM_MAXEFFDT_POSITION;
insert into etldev.STG_HCM_MAXEFFDT_XLAT select * from STG_HCM_MAXEFFDT_XLAT;
insert into etldev.STG_HCM_PRE_COMP select * from STG_HCM_PRE_COMP;
insert into etldev.STG_HCM_PRE_PER_ORG select * from STG_HCM_PRE_PER_ORG;

-- Cleanse data according to HR
update ETLDEV.ps_names set name = '-';
update ETLDEV.ps_pers_nid set national_id = '-';

update ETLDEV.ps_person_name set name = '-',first_name='-',last_name='-',last_name_srch='-',first_name_srch='-',
  name_formal='-',name_display='-',name_display_srch='-',name_psformat='-';

update ETLDEV.ps_personal_data set name = '-',first_name='-',last_name='-',last_name_srch='-',first_name_srch='-',
  name_formal='-',name_display='-';

-----
