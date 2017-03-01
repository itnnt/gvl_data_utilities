# load xlsx package
if (Sys.getenv("JAVA_HOME") != "") {
  Sys.setenv(JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_131")
}

source('EDM_set_common_input.R')

#
# set source data -------------------------------------------------------------------
#
processdt <- '2017-05-29'
dfu <- 'DFUWF-943'
checded_xlsx_file <- 'DFU_CheckDay_20170529.xlsx'
sheet_commission  = 0
sheet_kpi_sum     = 1
sheet_kpi_detail  = 2
script_before_after = TRUE
#------------------------------------------------------------------
# processdt <- '2017-05-08'
# dfu <- 'DFUWF-924'
# checded_xlsx_file <- 'DFU_CheckDay_20170508_TCA.xlsx'
# sheet_commission  = 1
# sheet_kpi_sum     = 2
# sheet_kpi_detail  = 3
# script_before_after = T
#------------------------------------------------------------------
# processdt <- '2017-03-20'
# dfu <- 'DFUWF-784'
# checded_xlsx_file <- 'DFU_CheckDay_20170320 - CMG.xlsx'
# sheet_commission  = 0
# sheet_kpi_sum     = 1
# sheet_kpi_detail  = 2
#************************************************************************************
env = 'COMPANY_EDM_VN'
# load checked data ####
kpisum <- read_excel(file.path(output, dfu, checded_xlsx_file), sheet = sheet_kpi_sum)
kpidetail <- read_excel(file.path(output, dfu, checded_xlsx_file), sheet = sheet_kpi_detail)
try(commission <- read_excel(file.path(output, dfu, checded_xlsx_file), sheet = sheet_commission), silent = TRUE)

cps.csv = file.path(output, dfu, paste('cps', 'csv', sep = '.'))
cpsd.csv = file.path(output, dfu, paste('cpsd', 'csv', sep = '.'))
cgbk.csv = file.path(output, dfu, paste('cgbk', 'csv', sep = '.'))
cgbk_no_ref.csv = file.path(output, dfu, paste('cgbk_no_ref', 'csv', sep = '.'))
group_detail.csv = file.path(output, dfu, paste('group_detail', 'csv', sep = '.'))

#_ CHECK TO REMOVE IF THE OUTPUT FILE IS ALREADY EXISTED ####
if (file.exists(cps.csv))
  file.remove(cps.csv)
if (file.exists(cpsd.csv))
  file.remove(cpsd.csv)
if (file.exists(cgbk.csv))
  file.remove(cgbk.csv)
if (file.exists(group_detail.csv))
  file.remove(group_detail.csv)
if (file.exists(cgbk_no_ref.csv))
  file.remove(cgbk_no_ref.csv)

out_put_patched_file = file.path(output, dfu, paste(checded_xlsx_file, 'sql', sep = '.'))

# format data
fieldnames <- names(kpisum)
fieldnames <- gsub('AGCode', 'AGENTCD', fieldnames)
fieldnames <- gsub('\\<KPI_\\>', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<KPI\\>', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<GVL\\>', 'TOOL', fieldnames)
fieldnames <- gsub('\\<IT\\>', 'EDM', fieldnames)
fieldnames <- gsub('ProductCode', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('productcd', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('\\<GROUP\\>', 'GROUPCD', fieldnames)
names(kpisum) <- fieldnames
kpisum$GROUPCD <- gsub('\\<PERSONEL\\>','PERSONNEL', kpisum$GROUPCD)
kpisum$GROUPCD <- gsub('\\<PERSONAL\\>','PERSONNEL', kpisum$GROUPCD)
kpisum_update <- dplyr::filter(kpisum, !is.na(kpisum$AGENTCD), EDM!=0 & !is.na(EDM))
kpisum <- dplyr::filter(kpisum, !is.na(kpisum$AGENTCD), TOOL!= 0 & !is.na(TOOL), EDM==0 | is.na(EDM))

fieldnames <- names(kpidetail)
fieldnames <- gsub('LEADER', 'LEADERCD', fieldnames)
fieldnames <- gsub('LEADERCDCD', 'LEADERCD', fieldnames)
fieldnames <- gsub('\\<REPORTINGCD\\>', 'LEADERCD', fieldnames)
names(kpidetail) <- fieldnames
if (!('LEADERCD' %in% fieldnames)) {
  kpidetail$LEADERCD = ''
  fieldnames <- names(kpidetail)
}
fieldnames <- gsub('AGCode', 'AGENTCD', fieldnames)
fieldnames <- gsub('RDOCNUM', 'POLICYCD', fieldnames)
fieldnames <- gsub('AGCODE', 'AGENTCD', fieldnames)
fieldnames <- gsub('KPICODE', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<KPI_\\>', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<KPI\\>', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<GVL\\>', 'TOOL', fieldnames)
fieldnames <- gsub('\\<IT\\>', 'EDM', fieldnames)
fieldnames <- gsub('ProductCode', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('productcd', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('\\<GROUP\\>', 'GROUPCD', fieldnames)
fieldnames <- gsub('Business date', 'BUSINESSDT', fieldnames)

print(fieldnames)
names(kpidetail) <- fieldnames
getdfu_channelcd <- paste(unique(kpidetail$AGENTCD), collapse = "','")
getdfu_channelcd <- sprintf("SELECT DISTINCT CHANNELCD FROM CMS_PRODUCERCHANNEL_M WHERE PRODUCERSOURCECD in ('%s')", getdfu_channelcd)
CHANNELCD <- execute_sql(getdfu_channelcd, env)
kpidetail$GROUPCD <- gsub('\\<PERSONEL\\>','PERSONNEL', kpidetail$GROUPCD)
kpidetail$GROUPCD <- gsub('\\<PERSONAL\\>','PERSONNEL', kpidetail$GROUPCD)
kpidetail_update <- dplyr::filter(kpidetail, !is.na(AGENTCD), EDM!=0 & !is.na(EDM))
kpidetail <- dplyr::filter(kpidetail, !is.na(AGENTCD), TOOL!= 0 & !is.na(TOOL), EDM==0 | is.na(EDM))

if (exists('commission')) {
  fieldnames <- names(commission)
  fieldnames <- gsub('AGCode', 'AGENTCD', fieldnames)
  fieldnames <- gsub('AGCODE', 'AGENTCD', fieldnames)
  fieldnames <- gsub('RDOCNUM', 'POLICYCD', fieldnames)
  fieldnames <- gsub('AGCODE', 'AGENTCD', fieldnames)
  fieldnames <- gsub('\\<LEADER\\>', 'LEADERCD', fieldnames)
  fieldnames <- gsub('KPI_', 'PRODNUNITCD', fieldnames)
  fieldnames <- gsub('KPICODE', 'PRODNUNITCD', fieldnames)
  fieldnames <- gsub('ProductCode', 'PRODUCTCD', fieldnames)
  fieldnames <- gsub('productcd', 'PRODUCTCD', fieldnames)
  fieldnames <- gsub('\\<GROUP\\>', 'GROUPCD', fieldnames)
  fieldnames <- gsub('Business date', 'BUSINESSDT', fieldnames)
  names(commission) <- fieldnames
  commission_update <- dplyr::filter(commission, !is.na(AGENTCD), EDM!=0 & !is.na(EDM))
}
#
# PREPARE PATCH FILE OUPUT
# 
save_to_sql_file('', out_put_patched_file, append = FALSE)
#
# GENERATE UPDATE COMMANDS ####
# 
# __1_update commission ####
if (exists('commission')) {
  if (nrow(commission_update)) {
    sql_updatecom <- "UPDATE CMS_PRODUCER_COMM SET COMMAMT=%s WHERE PRODUCERCOMMSEQ=%s"
    sql_selectcom <- "SELECT * FROM CMS_PRODUCER_COMM WHERE PRODUCERCOMMSEQ in (%s)"
    sql_getcom <- "
    SELECT PRODUCERCOMMSEQ FROM CMS_PRODUCER_COMM WHERE PRODUCTIONPROCESSDT = '{0}' 
    AND PRODUCERCD=(SELECT PRODUCERCD FROM CMS_PRODUCERCHANNEL_M WHERE PRODUCERSOURCECD='{1}')
    AND POLICYCD=(SELECT POLICYSEQ FROM CMS_POLICY_M WHERE POLICYCD='{2}')
    AND PRODUCTCD='{3}' 
    "
    seq <- vector(mode = 'character')
    updates <- vector(mode = 'character')
    for (i in 1:nrow(commission_update)) {
      temsql <- gsub("[{]0[}]", processdt, sql_getcom)
      temsql <- gsub("[{]1[}]", commission_update[i, 'AGENTCD'], temsql)
      temsql <- gsub("[{]2[}]", commission_update[i, 'POLICYCD'], temsql)
      temsql <- gsub("[{]3[}]", commission_update[i, 'PRODUCTCD'], temsql)
      # print(temsql)
      PRODUCERCOMMSEQ <- execute_sql(temsql, env)
      PRODUCERCOMMSEQ$UPDATE <- sprintf(sql_updatecom, commission_update[i, 'TOOL'], PRODUCERCOMMSEQ$PRODUCERCOMMSEQ)
      seq <- c(seq, PRODUCERCOMMSEQ$PRODUCERCOMMSEQ)
      updates <- c(updates, PRODUCERCOMMSEQ$UPDATE)
    }
    save_to_sql_file(sprintf(sql_selectcom, paste(unique(seq), collapse = ',')), out_put_patched_file, append = TRUE)
    save_to_sql_file(updates, out_put_patched_file, append = TRUE)
    save_to_sql_file(sprintf(sql_selectcom, paste(unique(seq), collapse = ',')), out_put_patched_file, append = TRUE)
  }
}

# __2_update kpi detail, breakup, sum ####
if (nrow(kpidetail_update)) {
  # ____find personnel detail ####
  sql_getprd <- "
  SELECT 
    CPSD.PRODNSUMMDTLSEQ, 
    CPSD.PRODNSUMMSEQ, 
    (SELECT SUM(PRODNVALUE) FROM CMS_PROD_SUMMARY_DTL WHERE PRODNSUMMSEQ=CPSD.PRODNSUMMSEQ) AS SUMVAL 
  FROM CMS_PROD_SUMMARY_DTL CPSD
  WHERE /*TRANSACTIONDT='{0}'
  AND*/ PRODUCERCD=(select producercd from cms_producerchannel_m where producersourcecd='{1}')
  AND POLICYCD=(SELECT POLICYSEQ FROM CMS_POLICY_M WHERE POLICYCD='{2}')
  AND PRODUCTCD='{3}'
  AND EXISTS (SELECT PRODNSUMMSEQ FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ=CPSD.PRODNSUMMSEQ AND PRODNUNITCD='{4}' AND PROCESSDT='{0}') 
  "
  
  for (i in 1:nrow(kpidetail_update)) {
    temsql <- gsub("[{]0[}]", processdt, sql_getprd)
    temsql <- gsub("[{]1[}]", kpidetail_update[i, 'AGENTCD'], temsql)
    temsql <- gsub("[{]2[}]", kpidetail_update[i, 'POLICYCD'], temsql)
    temsql <- gsub("[{]3[}]", kpidetail_update[i, 'PRODUCTCD'], temsql)
    temsql <- gsub("[{]4[}]", kpidetail_update[i, 'PRODNUNITCD'], temsql)
    
    PRODNSUMMDTLSEQ <- execute_sql(temsql, env)
    # print("-----------------------------------------")
    # print(temsql)
    # print("-----------------------------------------")
    kpidetail_update[i,'PRODNSUMMDTLSEQ'] = paste(PRODNSUMMDTLSEQ$PRODNSUMMDTLSEQ, collapse = ',')
    kpidetail_update[i,'PRODNSUMMSEQ'] = paste(unique(PRODNSUMMDTLSEQ$PRODNSUMMSEQ), collapse = ',')
    # print("------------------------------------------------------------------------------")
    # print(filter(kpidetail_update, as.numeric(PRODNSUMMSEQ)==as.numeric((kpidetail_update[i,'PRODNSUMMSEQ']))))
    # print("------------------------------------------------------------------------------")
    kpidetail_update[i,'SUMVAL'] = sum(unique(PRODNSUMMDTLSEQ$SUMVAL))
  }
  
  # sum a variable by group
  # a sum row refers to more than one detail rows 
  checksum <- aggregate(kpidetail_update$CHECK, by=list(PRODNSUMMSEQ=kpidetail_update$PRODNSUMMSEQ, GROUPCD=kpidetail_update$GROUPCD), FUN=sum) %>%
              setnames(c('PRODNSUMMSEQ', 'GROUPCD','SUMCHECK'))
  kpidetail_update <- merge(
    x = kpidetail_update,
    y = checksum,
    by.x = c("PRODNSUMMSEQ" ,'GROUPCD'),
    by.y = c("PRODNSUMMSEQ" ,'GROUPCD'),
    all.x = TRUE
  )
  
  sql_update <- "UPDATE CMS_PROD_SUMMARY_DTL SET PRODNVALUE=%s WHERE PRODNSUMMDTLSEQ in (%s)"
  mutate(kpidetail_update, kpidetail_update$CPSD <- sprintf(sql_update, TOOL, PRODNSUMMDTLSEQ))
  sql_update <- "UPDATE CMS_PRODUCTION_SUMMARY SET PRODNVALUE=%s WHERE PRODNSUMMSEQ in (%s)"
  mutate(kpidetail_update, kpidetail_update$CPS <- sprintf(sql_update, as.numeric(SUMVAL)+as.numeric(SUMCHECK), PRODNSUMMSEQ))
  
  #///////////////////////////////////////////////////////////////////////////
  # ____find breakup detail ####
  # 
  kpidetail_update$WEIGHTAGE =100*kpidetail_update$TOOL/kpidetail_update$SUMVAL
  sql_update <- "UPDATE CMS_GROUPPRODN_SUMM_BK SET PRODNVALUE=%s, WEIGHTAGE=%s WHERE GRPPRODNSUMMBKSEQ='%s'"
  sql_select <- "SELECT * FROM CMS_GROUPPRODN_SUMM_BK WHERE GRPPRODNSUMMBKSEQ in (%s)"
   
  for (c in CHANNELCD$CHANNELCD) {
    group_detail <- execute_store_procedure(sprintf("EXECUTE CMS_OTHER_PKG$SPC_GET_KPI_BREAKUP '%s', NULL, NULL, '%s'", processdt, c), env)
    group_detail <- tbl_df(group_detail)
    # save the new row to output file csv
    if (!(file.exists(group_detail.csv)))
      data.table::fwrite(group_detail, group_detail.csv, append = FALSE)
    else
      data.table::fwrite(group_detail, group_detail.csv, append = TRUE)
  }  
  if (file.exists(group_detail.csv)) {
    group_detail <- data.table::fread(file = group_detail.csv, showProgress = TRUE)
    if (nrow(group_detail)) { 
      for (i in 1:nrow(kpidetail_update)) {
        if (kpidetail_update[i, 'GROUPCD'] != 'PERSONNEL') {
          gd <- group_detail %>% 
            dplyr::filter(MAINPRODUCERSOURCECD==kpidetail_update[i, 'LEADERCD']) %>%
            dplyr::filter(SUBPRODUCERSOURCECD==kpidetail_update[i, 'AGENTCD']) %>%
            dplyr::filter(SUBPOLICYSOURCECD==kpidetail_update[i, 'POLICYCD']) %>%
            dplyr::filter(PRODNUNITCD==kpidetail_update[i, 'PRODNUNITCD']) %>%
            dplyr::filter(productcd==kpidetail_update[i, 'PRODUCTCD']) %>%
            dplyr::filter(MAINPRODNGROUPCD==kpidetail_update[i, 'GROUPCD']) %>%
            # dplyr::filter(as.numeric(MAINprodnvalue)==as.numeric(kpidetail_update[i, 'EDM'])) %>%
            dplyr::filter(SUBPRODNSUMMDTLSEQ==kpidetail_update[i, 'PRODNSUMMDTLSEQ']) %>%
            mutate(SQL = sprintf("SELECT GRPPRODNSUMMBKSEQ, PRODNVALUE, WEIGHTAGE FROM CMS_GROUPPRODN_SUMM_BK GSB WHERE PROCESSDT='%s' AND PRODUCERCD='%s' AND PRODNGROUPCD='%s' AND PRODNUNITCD='%s' AND PRODUCTCD='%s' AND PRODNVALUE=%s", PROCESSDT, MAINPRODUCERCD, MAINPRODNGROUPCD, PRODNUNITCD, productcd, MAINprodnvalue))
          
          #contiep####
          if (nrow(gd)) {
            GRPPRODNSUMMBKSEQ <- execute_sql(paste(gd$SQL, collapse = ' union all '), env)
            kpidetail_update[i, 'GRPPRODNSUMMBKSEQ'] = paste(GRPPRODNSUMMBKSEQ$GRPPRODNSUMMBKSEQ, collapse = ',')
          }
        }
      }
    }
  }
  if ('CGSBK' %in% names(kpidetail_update)) {
    mutate(kpidetail_update, kpidetail_update$CGSBK <- ifelse(is.na(GRPPRODNSUMMBKSEQ),'', sprintf(sql_update, as.numeric(SUMVAL)+as.numeric(SUMCHECK), WEIGHTAGE, GRPPRODNSUMMBKSEQ)))
  }
  #-------------------------------------
  #-------------------------------------
  #-------------------------------------
  #-------------------------------------
  if (script_before_after)
    save_to_sql_file(sprintf("SELECT * FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  save_to_sql_file(kpidetail_update$CPS, out_put_patched_file, append = TRUE)
  if (script_before_after)
    save_to_sql_file(sprintf("SELECT * FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  
  if (script_before_after)
    save_to_sql_file(sprintf("SELECT * FROM CMS_PROD_SUMMARY_DTL WHERE PRODNSUMMDTLSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMDTLSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  save_to_sql_file(kpidetail_update$CPSD, out_put_patched_file, append = TRUE)
  if (script_before_after)
    save_to_sql_file(sprintf("SELECT * FROM CMS_PROD_SUMMARY_DTL WHERE PRODNSUMMDTLSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMDTLSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  
  if ('CGSBK' %in% names(kpidetail_update)) {
    save_to_sql_file(kpidetail_update$CGSBK, out_put_patched_file, append = TRUE)
  }
  # save_to_sql_file(sprintf("SELECT * FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  # save_to_sql_file(kpidetail_update$CPS, out_put_patched_file, append = TRUE)
  # save_to_sql_file(sprintf("SELECT * FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  # 
  # save_to_sql_file(sprintf("SELECT * FROM CMS_PROD_SUMMARY_DTL WHERE PRODNSUMMDTLSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMDTLSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  # save_to_sql_file(kpidetail_update$CPSD, out_put_patched_file, append = TRUE)
  # save_to_sql_file(sprintf("SELECT * FROM CMS_PROD_SUMMARY_DTL WHERE PRODNSUMMDTLSEQ in (%s)", paste(unique(kpidetail_update$PRODNSUMMDTLSEQ), collapse = ',')), out_put_patched_file, append = TRUE)
  
}
#
# load edm kpi data ####
# 
# group <- get_banca_kpi_group(env, processdt)
# group_detailbankstaff <- get_banca_kpi_group_detailbankstaff(env, processdt)
# group_detailinhouse <- get_banca_kpi_group_detailinhouse(env, processdt)
# personal <- get_banca_kpi_personal(env, processdt)
# personal_detail <- get_banca_kpi_personal_detail(env, processdt)
# personal_detail_team <- get_banca_kpi_personal_detail_team(env, processdt)
# team <- get_banca_kpi_team(env, processdt)
#
kpidef <- tbl_df(get_kpi(env))
products <- tbl_df(get_product(env))

# c1 <- names(sample_cps)
# c2 <- names(sample_cpsbk)
# intersect(c1, c2)
# setdiff(c1, c2) # element in c1 not c2
# setdiff(c2, c1) # element in c2 not c1

# patching production summary ####
for (agentcd in unique(kpidetail$AGENTCD)) {
  temp <- dplyr::filter(kpidetail, kpidetail$AGENTCD==agentcd)
  cps <-  "SELECT TOP 1 * FROM CMS_PRODUCTION_SUMMARY where PROCESSDT='{0}' AND PRODUCERCD=(select producercd from cms_producerchannel_m where producersourcecd='{1}')"
  cps <- gsub("[{]0[}]", processdt, cps)
  
  cpsd <- "
  SELECT TOP 1 * FROM CMS_PROD_SUMMARY_DTL CPSD
  where /*TRANSACTIONDT='{0}'
  AND*/ PRODUCERCD=(select producercd from cms_producerchannel_m where producersourcecd='{1}')
  AND POLICYCD=(SELECT POLICYSEQ FROM CMS_POLICY_M WHERE POLICYCD='{2}')
  AND PRODUCTCD='{3}'
  AND EXISTS (SELECT PRODNSUMMSEQ FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ=CPSD.PRODNSUMMSEQ AND PRODNUNITCD='{4}' AND PROCESSDT='{0}') 
  "
  cpsd <- gsub("[{]0[}]", processdt, cpsd)
  
  
  sample_cps1 <- execute_sql(sprintf("SELECT TOP 1 * FROM CMS_PROD_SUMMARY_DTL where TRANSACTIONDT='%s'", processdt), env)
 
  temp_sum <- temp %>% select(-POLICYCD) %>% group_by(AGENTCD,LEADERCD,PRODNUNITCD,PRODUCTCD,GROUPCD) %>% summarise_each(funs(sum))
  for (i in 1:nrow(temp_sum)) {
    if (as.character(temp_sum[i, 'GROUPCD']) %in% c ('PERSONAL', 'PERSONNEL')) {
      # _create a new row for CMS_PRODUCTION_SUMMARY: PERSONAL ####
      sample_cps <- execute_sql(gsub("[{]1[}]", temp_sum[i, 'AGENTCD'], cps), env)
    } else {
      # _create a new row for CMS_PRODUCTION_SUMMARY: GROUP ####
      sample_cps <- execute_sql(gsub("[{]1[}]", temp_sum[i, 'LEADERCD'], cps), env)
    }
    sample_cps <- gen_cps_row(sample_cps, temp_sum[i,], kpidef, products)
    # save the new row to output file csv
    if (!(file.exists(cps.csv)))
      data.table::fwrite(sample_cps, cps.csv, append = FALSE)
    else
      data.table::fwrite(sample_cps, cps.csv, append = TRUE)
  }
  
  for (i in 1:nrow(temp)) {
    if (as.character(temp[i, 'GROUPCD']) %in% c ('PERSONAL', 'PERSONNEL')) {
      # _create a new row for CMS_PROD_SUMMARY_DTL ####
      sample_cps1$CREATEDBY = 'TUNGNGUYEN'
      sample_cps1$UPDATEDBY = 'TUNGNGUYEN'
      sample_cps1$PRODNVALUE = temp[i, 'TOOL']
      sample_cps1$PRODNVALUE <- format(sample_cps1$PRODNVALUE, scientific=FALSE)
      sample_cps1$PRODUCTCD = temp[i, 'PRODUCTCD']
      sample_cps1$PRODUCERCD=sprintf("(select producercd from cms_producerchannel_m where producersourcecd='%s')", temp[i, 'AGENTCD'])
      sample_cps1$POLICYCD = sprintf("(SELECT POLICYSEQ FROM CMS_POLICY_M WHERE POLICYCD='%s')", temp[i, 'POLICYCD'])
      scp = paste('@NEWSEQ', temp[i, 'PRODNUNITCD'], agentcd, temp[i, 'GROUPCD'], temp[i, 'PRODUCTCD'], sep='_')
      sample_cps1$PRODNSUMMSEQ = scp
      # save the new row to output file csv
      if (!(file.exists(cps.csv)))
        data.table::fwrite(sample_cps1, cpsd.csv, append = FALSE)
      else
        data.table::fwrite(sample_cps1, cpsd.csv, append = TRUE)
    } 
    
    if (!(as.character(temp[i, 'GROUPCD']) %in% c ('PERSONAL', 'PERSONNEL'))) {
      # _create a new row for CMS_GROUPPRODN_SUMM_BK ####
      sample_cps2 <- dplyr::tbl_df(execute_sql(gsub("[{]1[}]", temp[i, 'LEADERCD'], cps), env)) %>%
        select(-c(PRODNTYPECD,GROUPBATCHDT,OFFICECD,ZONECD,REGIONCD,AGENCYCD,ORIGINALDT))
      sample_cps2$PRODNSUMMSEQ = NA
      sample_cps2$CREATEDBY = 'TUNGNGUYEN'
      sample_cps2$UPDATEDBY = 'TUNGNGUYEN'
      # assign group detail calculated based on personnel
      sample_cps2$PRODNGROUPCD = temp[i, 'GROUPCD']
      sample_cps2$SRCPRODNGROUPCD = 'PERSONNEL'
      sample_cps2$PRODNVALUE = temp[i, 'TOOL']
      sample_cps2$PRODNVALUE <- format(sample_cps2$PRODNVALUE, scientific=FALSE)
      sample_cps2$PRODNUNITCD = temp[i, 'PRODNUNITCD']
      sample_cps2$PRODNBASISCD = dplyr::select(dplyr::filter(kpidef, PRODNUNITCD == as.character(temp[i, 'PRODNUNITCD'])), PRODNUNITBASISCD)
      if (!(is.na(temp[i, 'PRODUCTCD']))) {
        product = dplyr::select(dplyr::filter(products, PRODUCTCD == as.character(temp[i, 'PRODUCTCD'])), PRODUCTSEQ, PRODUCTCD)
        sample_cps2$PRODUCTSEQ = product$PRODUCTSEQ
        sample_cps2$PRODUCTCD = product$PRODUCTCD
      } else {
        sample_cps2$PRODUCTSEQ = 0
        sample_cps2$PRODUCTCD = 'NULL'
      }
      
      baseondetail <- dplyr::filter(kpidetail, 
                                    AGENTCD==agentcd, 
                                    PRODNUNITCD==as.character(temp[i, 'PRODNUNITCD']), 
                                    PRODUCTCD==as.character(temp[i, 'PRODUCTCD']), 
                                    GROUPCD=='PERSONNEL'
                                    )
      if (nrow(baseondetail) > 0) {
        scp = paste('@NEWSEQ', baseondetail$PRODNUNITCD, baseondetail$AGENTCD, baseondetail$GROUPCD, baseondetail$PRODUCTCD, sep='_')
        sample_cps2$PRODNSUMMSEQ = unique(scp)
        sample_cps2$WEIGHTAGE = temp[i, 'TOOL']/baseondetail$TOOL*100
      } else {
        print("searching base on detail from database")
        temcpsd <- gsub("[{]1[}]", temp[i, 'AGENTCD'], cpsd)
        temcpsd <- gsub("[{]2[}]", temp[i, 'POLICYCD'], temcpsd)
        temcpsd <- gsub("[{]3[}]", temp[i, 'PRODUCTCD'], temcpsd)
        temcpsd <- gsub("[{]4[}]", temp[i, 'PRODNUNITCD'], temcpsd)
        print(temcpsd)
        baseondetail <- execute_sql(temcpsd, env)
        if (nrow(baseondetail) > 0) {
          scp = baseondetail$PRODNSUMMSEQ
          sample_cps2$PRODNSUMMSEQ = unique(scp)
          sample_cps2$WEIGHTAGE = temp[i, 'TOOL']/baseondetail$PRODNVALUE*100
        }
      }
     
      # save the new row to output file csv
      if (!is.na(sample_cps2$PRODNSUMMSEQ)) {
        if (!(file.exists(cgbk.csv)))
          data.table::fwrite(sample_cps2, cgbk.csv, append = FALSE)
        else
          data.table::fwrite(sample_cps2, cgbk.csv, append = TRUE)
      } else {
        if (!(file.exists(cgbk_no_ref.csv)))
          data.table::fwrite(sample_cps2, cgbk_no_ref.csv, append = FALSE)
        else
          data.table::fwrite(sample_cps2, cgbk_no_ref.csv, append = TRUE)
      }
    }
  }
}
#
# GENERATING PATCHING SCRIPT ####
# 
if (file.exists(cps.csv)) {
  t1 <- data.table::fread(file = cps.csv, showProgress = TRUE)
  t2 <- t1
  t1 <- select(t1,-c(PRODNSUMMSEQ, PRODNVALUE))
  t1 <- unique(t1)
  sqls <- vector(mode = 'character')
  for (i in 1:nrow(t1)) {
    t1[i, 'PRODNVALUE'] <- sum(select(filter(t2, as.character(SCOPE_IDENTITY)==as.character(t1[i, 'SCOPE_IDENTITY'])), PRODNVALUE))
    values <- paste(select(t1, -c(SCOPE_IDENTITY))[i], collapse = "','")
    sql1 <- "INSERT INTO CMS_PRODUCTION_SUMMARY (%s) VALUES ('%s')"
    sql1 <- sprintf(sql1, paste(colnames(select(t1, -c(SCOPE_IDENTITY))), collapse = ','), values)
    sql1 <- gsub("'NA'", 'NULL', sql1)
    sql1 <- gsub("''", 'NULL', sql1)
    sql1 <- paste(sql1, t1[i, 'SCOPE_IDENTITY'], sep = '\n')
    sqls <- c(sqls, sql1)
  }
  save_to_sql_file(paste(sqls, collapse = '\n'), out_put_patched_file, append = TRUE)
  
  if (script_before_after) {
    t1 <- mutate(t1, SELECT <- sprintf("SELECT * FROM CMS_PRODUCTION_SUMMARY WHERE PROCESSDT='%s' AND PRODNUNITCD='%s' AND PRODUCERCD='%s' AND PRODUCTCD='%s'", PROCESSDT, PRODNUNITCD, PRODUCERCD, PRODUCTCD))
    save_to_sql_file(paste(t1$SELECT, collapse = ' union all \n'), out_put_patched_file, append = TRUE)
  }
}

if (file.exists(cpsd.csv)) {
  t1 <- data.table::fread(file = cpsd.csv, showProgress = TRUE)
  sqls <- vector(mode = 'character')
  for (i in 1:nrow(t1)) {
    values1 <- paste(t1[i, PRODNSUMMSEQ:PRODUCERCD], collapse = ",")
    values2 <- paste(t1[i, PRODNVALUE:ROWVERSION], collapse = "','")
    values <- sprintf("%s,'%s'",values1, values2)
    sql1 <- "INSERT INTO CMS_PROD_SUMMARY_DTL (%s) VALUES (%s)"
    sql1 <- sprintf(sql1, paste(colnames(select(t1, -PRODNSUMMDTLSEQ)), collapse = ','), values)
    sql1 <- gsub("'NA'", 'NULL', sql1)
    sql1 <- gsub("''", 'NULL', sql1)
    sqls <- c(sqls, sql1)
  }
  save_to_sql_file(paste(sqls, collapse = '\n'), out_put_patched_file, append = TRUE)
  if (script_before_after) {
    t1 <- mutate(t1, SELECT = sprintf("SELECT * FROM CMS_PROD_SUMMARY_DTL WHERE TRANSACTIONDT='%s' AND PRODUCERCD=%s AND PRODUCTCD='%s'", TRANSACTIONDT, PRODUCERCD, PRODUCTCD))
    save_to_sql_file(paste(t1$SELECT, collapse = ' union all \n'), out_put_patched_file, append = TRUE)
  }
}
  
if (file.exists(cgbk.csv)) {
  t1 <- data.table::fread(file = cgbk.csv, showProgress = TRUE)
  sqls <- vector(mode = 'character')
  for (i in 1:nrow(t1)) {
    values1 <- paste(t1[i, PRODNSUMMSEQ], collapse = ",")
    values2 <- paste(t1[i, PRODNUNITCD:WEIGHTAGE], collapse = "','")
    values <- sprintf("%s,'%s'",values1, values2)
    sql1 <- "INSERT INTO CMS_GROUPPRODN_SUMM_BK (%s) VALUES (%s)"
    sql1 <- sprintf(sql1, paste(colnames(t1), collapse = ','), values)
    sql1 <- gsub("'NA'", 'NULL', sql1)
    sql1 <- gsub("''", 'NULL', sql1)
    sqls <- c(sqls, sql1)
  }
  save_to_sql_file(paste(sqls, collapse = '\n'), out_put_patched_file, append = TRUE)
  if (script_before_after) {
  t1 <- mutate(t1, SELECT <- sprintf("SELECT * FROM CMS_GROUPPRODN_SUMM_BK WHERE PROCESSDT='%s' AND PRODNUNITCD='%s' AND PRODUCERCD='%s' AND PRODUCTCD='%s'", PROCESSDT, PRODNUNITCD, PRODUCERCD, PRODUCTCD))
  save_to_sql_file(paste(t1$SELECT, collapse = ' union all \n'), out_put_patched_file, append = TRUE)
  }
}

rm(list = ls())

