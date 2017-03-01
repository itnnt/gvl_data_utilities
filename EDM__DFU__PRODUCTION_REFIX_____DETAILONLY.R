library(readxl)
source('EDM_set_common_input.R')

dfuid = "DFUWF-800"
processdt = '2017-03-27'
user_checked_file = "DFU_CheckDay_20170327.xlsx"
env='COMPANY_EDM_VN'

detail_user <- read_excel(sprintf("S:/IT/Share to others/tung/%s/%s", dfuid, user_checked_file), sheet = "KPI_DETAIL")

group_detail <- get_tiedagency_kpi_group_detail(env, processdt)
group_detail_needtobepatched <- dplyr::filter(group_detail, 
                                              group_detail$MAINPRODUCERSOURCECD %in% c('AG003377','AG003137','AG003132'),
                                              group_detail$PRODNUNITCD %in% detail_user$KPICODE,
                                              group_detail$productcd %in% detail_user$productcd,
                                              group_detail$SUBPOLICYSOURCECD %in% detail_user$POLICYCD
                                              )

for (i in 1:nrow(group_detail_needtobepatched)) {
  sql_getgrpprodnsumbkseq = sprintf("SELECT GRPPRODNSUMMBKSEQ FROM CMS_GROUPPRODN_SUMM_BK 
                                                    WHERE PROCESSDT='%s'
                                                    AND PRODUCERCD ='%s'
                                                    and PRODNUNITCD ='%s'
                                                    and PRODNGROUPCD ='%s'
                                                    and PRODUCTCD='%s'", 
                                                   group_detail_needtobepatched[i, 'PROCESSDT'],
                                                   group_detail_needtobepatched[i, 'MAINPRODUCERCD'],
                                                   group_detail_needtobepatched[i, 'PRODNUNITCD'],
                                                   group_detail_needtobepatched[i, 'MAINPRODNGROUPCD'],
                                                   group_detail_needtobepatched[i, 'productcd']
                                                   )
  GRPPRODNSUMMBKSEQ <- execute_sql(sql_getgrpprodnsumbkseq, env)
  group_detail_needtobepatched[i,'GRPPRODNSUMMBKSEQ'] = paste(GRPPRODNSUMMBKSEQ, collapse = ',')
  
}