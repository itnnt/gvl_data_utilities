source('EDM_set_common_input.R')

# set source data -------------------------------------------------------------------
processdt <- '2017-03-09'
dfu <- 'DFUWF-786'
checded_xlsx_file <- 'DFU_CheckDay_20170309 _TCA.xlsx'
sheet_commission  = 1
sheet_kpi_sum     = 2
sheet_kpi_detail  = 3
#************************************************************************************
env = 'COMPANY_EDM_VN'
# load checked data ####
kpisum <- read_excel(file.path(output, dfu, checded_xlsx_file), sheet = sheet_kpi_sum)
kpidetail <- read_excel(file.path(output, dfu, checded_xlsx_file), sheet = sheet_kpi_detail)
try(commission <- read_excel(file.path(output, dfu, checded_xlsx_file), sheet = sheet_commission), silent = TRUE)

cps.csv = file.path(output, dfu, paste('cps', 'csv', sep = '.'))
cpsd.csv = file.path(output, dfu, paste('cpsd', 'csv', sep = '.'))
cgbk.csv = file.path(output, dfu, paste('cgbk', 'csv', sep = '.'))
out_put_patched_file = file.path(output, dfu, paste(checded_xlsx_file, 'sql', sep = '.'))

# format data
fieldnames <- names(kpisum)
fieldnames <- gsub('AGCode', 'AGENTCD', fieldnames)
fieldnames <- gsub('KPI_', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('KPI', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('ProductCode', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('productcd', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('\\<GROUP\\>', 'GROUPCD', fieldnames)
names(kpisum) <- fieldnames
kpisum$GROUPCD <- gsub('\\<PERSONEL\\>','PERSONNEL', kpisum$GROUPCD)
kpisum$GROUPCD <- gsub('\\<PERSONAL\\>','PERSONNEL', kpisum$GROUPCD)
kpisum_update <- dplyr::filter(kpisum, !is.na(kpisum$AGENTCD), EDM!=0 & !is.na(EDM))
kpisum <- dplyr::filter(kpisum, !is.na(kpisum$AGENTCD), TOOL!= 0 & !is.na(TOOL), EDM==0 | is.na(EDM))

fieldnames <- names(kpidetail)
if (!('LEADERCD' %in% fieldnames)) {
  kpidetail$LEADERCD = ''
  fieldnames <- names(kpidetail)
}
fieldnames <- gsub('AGCode', 'AGENTCD', fieldnames)
fieldnames <- gsub('RDOCNUM', 'POLICYCD', fieldnames)
fieldnames <- gsub('AGCODE', 'AGENTCD', fieldnames)
fieldnames <- gsub('LEADER', 'LEADERCD', fieldnames)
fieldnames <- gsub('LEADERCDCD', 'LEADERCD', fieldnames)
fieldnames <- gsub('KPI_', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('KPI', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('ProductCode', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('productcd', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('\\<GROUP\\>', 'GROUPCD', fieldnames)
fieldnames <- gsub('Business date', 'BUSINESSDT', fieldnames)
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
  fieldnames <- gsub('RDOCNUM', 'POLICYCD', fieldnames)
  fieldnames <- gsub('AGCODE', 'AGENTCD', fieldnames)
  fieldnames <- gsub('\\<LEADER\\>', 'LEADERCD', fieldnames)
  fieldnames <- gsub('KPI_', 'PRODNUNITCD', fieldnames)
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
    
    rm(sql_updatecom, sql_selectcom, sql_getcom, temsql, i, PRODUCERCOMMSEQ, seq, updates)
  }
}
if (nrow(kpisum_update)) {
  sql_update <- "UPDATE CMS_PRODUCTION_SUMMARY SET PRODNVALUE=%s WHERE PRODNSUMMSEQ=%s"
  sql_select <- "SELECT * FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ in (%s)"
  sql_getprd <- "
  SELECT PRODNSUMMSEQ FROM CMS_PRODUCTION_SUMMARY 
  WHERE PROCESSDT='{0}' 
  AND PRODNUNITCD='{1}' 
  AND PRODUCTCD='{2}' 
  AND PRODUCERCD=(SELECT PRODUCERCD FROM CMS_PRODUCERCHANNEL_M WHERE PRODUCERSOURCECD='{3}') 
  AND PRODNGROUPCD='{4}' 
  "
  seq <- vector(mode = 'character')
  updates <- vector(mode = 'character')
  for (i in 1:nrow(kpisum_update)) {
    temsql <- gsub("[{]0[}]", processdt, sql_getprd)
    temsql <- gsub("[{]1[}]", kpisum_update[i, 'PRODNUNITCD'], temsql)
    temsql <- gsub("[{]2[}]", kpisum_update[i, 'PRODUCTCD'], temsql)
    temsql <- gsub("[{]3[}]", kpisum_update[i, 'AGENTCD'], temsql)
    temsql <- gsub("[{]4[}]", kpisum_update[i, 'GROUPCD'], temsql)
    # print(temsql)
    PRODNSUMMSEQ <- execute_sql(temsql, env)
    PRODNSUMMSEQ$UPDATE <- sprintf(sql_update, kpisum_update[i, 'TOOL'], PRODNSUMMSEQ$PRODNSUMMSEQ)
    seq <- c(seq, PRODNSUMMSEQ$PRODNSUMMSEQ)
    updates <- c(updates, PRODNSUMMSEQ$UPDATE)
  }
  save_to_sql_file(sprintf(sql_select, paste(unique(seq), collapse = ',')), out_put_patched_file, append = TRUE)
  save_to_sql_file(updates, out_put_patched_file, append = TRUE)
  save_to_sql_file(sprintf(sql_select, paste(unique(seq), collapse = ',')), out_put_patched_file, append = TRUE)
  
  rm(sql_update, sql_select, sql_getprd, temsql, i, PRODNSUMMSEQ, seq, updates)
}

if (nrow(kpidetail_update)) {
  # _update personnel detail ####
  sql_update <- "UPDATE CMS_PROD_SUMMARY_DTL SET PRODNVALUE=%s WHERE PRODNSUMMDTLSEQ=%s"
  sql_select <- "SELECT * FROM CMS_PROD_SUMMARY_DTL WHERE PRODNSUMMDTLSEQ in (%s)"
  sql_getprd <- "
  SELECT CPSD.PRODNSUMMDTLSEQ FROM CMS_PROD_SUMMARY_DTL CPSD
  WHERE TRANSACTIONDT='{0}'
  AND PRODUCERCD=(select producercd from cms_producerchannel_m where producersourcecd='{1}')
  AND POLICYCD=(SELECT POLICYSEQ FROM CMS_POLICY_M WHERE POLICYCD='{2}')
  AND PRODUCTCD='{3}'
  AND EXISTS (SELECT PRODNSUMMSEQ FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ=CPSD.PRODNSUMMSEQ AND PRODNUNITCD='{4}') 
  "
  seq <- vector(mode = 'character')
  updates <- vector(mode = 'character')
  for (i in 1:nrow(kpidetail_update)) {
    temsql <- gsub("[{]0[}]", processdt, sql_getprd)
    temsql <- gsub("[{]1[}]", kpidetail_update[i, 'AGENTCD'], temsql)
    temsql <- gsub("[{]2[}]", kpidetail_update[i, 'POLICYCD'], temsql)
    temsql <- gsub("[{]3[}]", kpidetail_update[i, 'PRODUCTCD'], temsql)
    temsql <- gsub("[{]4[}]", kpidetail_update[i, 'PRODNUNITCD'], temsql)
    
    PRODNSUMMDTLSEQ <- execute_sql(temsql, env)
    PRODNSUMMDTLSEQ$UPDATE <- sprintf(sql_update, kpidetail_update[i, 'TOOL'], PRODNSUMMDTLSEQ$PRODNSUMMDTLSEQ)
    seq <- c(seq, PRODNSUMMDTLSEQ$PRODNSUMMDTLSEQ)
    updates <- c(updates, PRODNSUMMDTLSEQ$UPDATE)
  }
  save_to_sql_file(sprintf(sql_select, paste(unique(seq), collapse = ',')), out_put_patched_file, append = TRUE)
  save_to_sql_file(updates, out_put_patched_file, append = TRUE)
  save_to_sql_file(sprintf(sql_select, paste(unique(seq), collapse = ',')), out_put_patched_file, append = TRUE)
  
  rm(sql_update, sql_select, sql_getprd, temsql, i, PRODNSUMMDTLSEQ, seq, updates)
  
  # _update breakup detail ####
  sql_update <- "UPDATE CMS_GROUPPRODN_SUMM_BK SET PRODNVALUE=%s WHERE GRPPRODNSUMMBKSEQ=%s"
  sql_select <- "SELECT * FROM CMS_GROUPPRODN_SUMM_BK WHERE GRPPRODNSUMMBKSEQ in (%s)"
  for (c in CHANNELCD$CHANNELCD) {
    group_detail <- execute_store_procedure(sprintf("EXECUTE CMS_OTHER_PKG$SPC_GET_KPI_BREAKUP '%s', NULL, NULL, '%s'", processdt, c), env)
    group_detail <- tbl_df(group_detail)
    if (nrow(group_detail))  
      for (i in 1:nrow(kpidetail_update)) {
        gd <- group_detail %>% 
          dplyr::filter(SUBPRODUCERSOURCECD==kpidetail_update[i, 'AGENTCD']) %>%
          dplyr::filter(group_detail$SUBPOLICYSOURCECD==kpidetail_update[i, 'POLICYCD']) %>%
          dplyr::filter(group_detail$PRODNUNITCD==kpidetail_update[i, 'POLICYCD']) %>%
          dplyr::filter(group_detail$productcd==kpidetail_update[i, 'PRODUCTCD']) %>%
          dplyr::filter(group_detail$MAINPRODNGROUPCD==kpidetail_update[i, 'GROUPCD']) %>%
          dplyr::filter(as.numeric(group_detail$MAINprodnvalue)==as.numeric(kpidetail_update[i, 'EDM'])) 
        gd$SQL <- sprintf("SELECT GRPPRODNSUMMBKSEQ FROM CMS_GROUPPRODN_SUMM_BK GSB WHERE PROCESSDT='%s' AND PRODUCERCD='%s' AND PRODNGROUPCD='%s' AND PRODNUNITCD='%s' AND PRODUCTCD='%s' AND PRODNVALUE=%s", PROCESSDT, MAINPRODUCERCD, MAINPRODNGROUPCD, PRODNUNITCD, productcd, MAINprodnvalue)
      }
  }
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

# prepare sql query for retrieving sample data ####
cps <-
  "SELECT TOP 1 * FROM CMS_PRODUCTION_SUMMARY where PROCESSDT='{0}' AND PRODUCERCD=(select producercd from cms_producerchannel_m where producersourcecd='{1}')"
cps <- gsub("[{]0[}]", processdt, cps)

cpsd <- "
SELECT * FROM CMS_PROD_SUMMARY_DTL CPSD
where TRANSACTIONDT='{0}'
AND PRODUCERCD=(select producercd from cms_producerchannel_m where producersourcecd='{1}')
AND POLICYCD=(SELECT POLICYSEQ FROM CMS_POLICY_M WHERE POLICYCD='{2}')
AND PRODUCTCD='{3}'
AND EXISTS (SELECT PRODNSUMMSEQ FROM CMS_PRODUCTION_SUMMARY WHERE PRODNSUMMSEQ=CPSD.PRODNSUMMSEQ AND PRODNUNITCD='{4}') 
"
cpsd <- gsub("[{]0[}]", processdt, cpsd)

# c1 <- names(sample_cps)
# c2 <- names(sample_cpsbk)
# intersect(c1, c2)
# setdiff(c1, c2) # element in c1 not c2
# setdiff(c2, c1) # element in c2 not c1

# patching production summary ####
#_ CHECK TO REMOVE IF THE OUTPUT FILE IS ALREADY EXISTED ####
if (file.exists(cps.csv))
  file.remove(cps.csv)
#_ generate data for production summary table ####
for (agentcd in unique(kpisum$AGENTCD)) {
  # find an appropriate sample row
  sample_cps <- execute_sql(gsub("[{]1[}]", agentcd, cps), env)
  # filter by agentcd
  temp <- dplyr::filter(kpisum, kpisum$AGENTCD == agentcd)
  # create each new row based on the sample row
  for (i in 1:nrow(temp)) {
    sample_cps$PRODNUNITCD = temp[i, 'PRODNUNITCD']
    sample_cps$PRODNBASISCD = dplyr::select(dplyr::filter(kpidef, PRODNUNITCD == as.character(temp[i, 'PRODNUNITCD'])), PRODNUNITBASISCD)
    if (!(is.na(temp[i, 'PRODUCTCD']))) {
      product = dplyr::select(dplyr::filter(products, PRODUCTCD == as.character(temp[i, 'PRODUCTCD'])), PRODUCTSEQ, PRODUCTCD)
      sample_cps$PRODUCTSEQ = product$PRODUCTSEQ
      sample_cps$PRODUCTCD = product$PRODUCTCD
    } else {
      sample_cps$PRODUCTSEQ = 0
      sample_cps$PRODUCTCD = 'NULL'
    }
    
    if (temp[i, 'GROUPCD'] %in% c ('PERSONAL', 'PERSONNEL')) {
      sample_cps$PRODNGROUPCD = 'PERSONNEL'
      sample_cps$PRODNTYPECD = 'PERSONNEL'
    } else {
      sample_cps$PRODNGROUPCD = temp[i, 'GROUPCD']
      sample_cps$PRODNTYPECD = 'GROUP'
    }
    
    sample_cps$PRODNVALUE = temp[i, 'TOOL']
    sample_cps$CREATEDBY = 'TUNGNGUYEN'
    sample_cps$UPDATEDBY = 'TUNGNGUYEN'
    scp = paste('@NEWSEQ', sample_cps$PRODNUNITCD, agentcd, sample_cps$PRODNGROUPCD, sample_cps$PRODUCTCD, sep='_')
    sample_cps$SCOPE_IDENTITY=paste("DECLARE", scp, 'NUMERIC(30,0)=SCOPE_IDENTITY();', sep=' ')
    
    # save the new row to output file csv
    if (!(file.exists(cps.csv)))
      data.table::fwrite(sample_cps, cps.csv, append = FALSE)
    else
      data.table::fwrite(sample_cps, cps.csv, append = TRUE)
  }
}

# patching for detail ####
#_ CHECK TO REMOVE IF THE OUTPUT FILE IS ALREADY EXISTED ####
if (file.exists(cpsd.csv))
  file.remove(cpsd.csv)

if (file.exists(cgbk.csv))
  file.remove(cgbk.csv)
for (agentcd in unique(kpidetail$AGENTCD)) {
  temp <- dplyr::filter(kpidetail, kpidetail$AGENTCD==agentcd)
  # find an appropriate sample row for break up (use the same to summary)
  sample_cps1 <- "SELECT TOP 1 * FROM CMS_PROD_SUMMARY_DTL CPSD where TRANSACTIONDT='{0}' AND PRODUCERCD=(select producercd from cms_producerchannel_m where producersourcecd='{1}')"
  sample_cps1 <- gsub("[{]0[}]", processdt, sample_cps1)
  sample_cps1 <- gsub("[{]1[}]", agentcd, sample_cps1)
  sample_cps1 <- execute_sql(sample_cps1, env)
  # find an appropriate sample row for break up (use the same to summary)
  sample_cps2 <- dplyr::tbl_df(execute_sql(gsub("[{]1[}]", temp[1, 'LEADERCD'], cps), env)) %>%
    select(-c(PRODNTYPECD,GROUPBATCHDT,OFFICECD,ZONECD,REGIONCD,AGENCYCD,ORIGINALDT))
  for (i in 1:nrow(temp)) {
    #_ patching for pesonnel detail ####
    if (as.character(temp[i, 'GROUPCD']) %in% c ('PERSONAL', 'PERSONNEL')) {
      # create a new row for personal kpi detail
      # sample_cps1$CREATEDBY = 'TUNGNGUYEN'
      # sample_cps1$UPDATEDBY = 'TUNGNGUYEN'
      sample_cps1$PRODNVALUE = temp[i, 'TOOL']
      sample_cps1$PRODUCTCD = temp[i, 'PRODUCTCD']
      sample_cps1$POLICYCD = sprintf("(SELECT POLICYSEQ FROM CMS_POLICY_M WHERE POLICYCD='%s')", temp[i, 'POLICYCD'])
      # find if this detail row is referred to a sum row in the patched file
      baseondetail <- dplyr::filter(kpisum, AGENTCD==agentcd, PRODNUNITCD==as.character(temp[i, 'PRODNUNITCD']), PRODUCTCD==as.character(temp[i, 'PRODUCTCD']), GROUPCD=='PERSONNEL')
      if (nrow(baseondetail) > 0) {
        scp = paste('@NEWSEQ', baseondetail$PRODNUNITCD, baseondetail$AGENTCD, baseondetail$GROUPCD, baseondetail$PRODUCTCD, sep='_')
      } else {
        # there is no sum row in the patched file, we should find it from edm database
        print("searching base on detail from database")
        temcpsd <- gsub("[{]1[}]", temp[i, 'AGENTCD'], cpsd)
        temcpsd <- gsub("[{]2[}]", temp[i, 'POLICYCD'], temcpsd)
        temcpsd <- gsub("[{]3[}]", temp[i, 'PRODUCTCD'], temcpsd)
        temcpsd <- gsub("[{]4[}]", temp[i, 'PRODNUNITCD'], temcpsd)
        print(temcpsd)
        baseondetail <- execute_sql(temcpsd, env)
        scp = baseondetail$PRODNSUMMSEQ
      }
      sample_cps1$PRODNSUMMSEQ = scp
    } else {
    #_ patching for group detail ####
      # create a new row for group kpi detail
      sample_cps2$CREATEDBY = 'TUNGNGUYEN'
      sample_cps2$UPDATEDBY = 'TUNGNGUYEN'
      # assign group detail calculated based on personnel
      sample_cps2$PRODNGROUPCD = temp[i, 'GROUPCD']
      sample_cps2$SRCPRODNGROUPCD = 'PERSONNEL'
      sample_cps2$WEIGHTAGE = 100
      sample_cps2$PRODNVALUE = temp[i, 'TOOL']
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
      
      baseondetail <- dplyr::filter(kpisum, AGENTCD==agentcd, PRODNUNITCD==as.character(temp[i, 'PRODNUNITCD']), PRODUCTCD==as.character(temp[i, 'PRODUCTCD']), GROUPCD=='PERSONNEL')
      if (nrow(baseondetail) > 0) {
        scp = paste('@NEWSEQ', baseondetail$PRODNUNITCD, baseondetail$AGENTCD, baseondetail$PRODNGROUPCD, baseondetail$PRODUCTCD, sep='_')
      } else {
        print("searching base on detail from database")
        temcpsd <- gsub("[{]1[}]", temp[i, 'AGENTCD'], cpsd)
        temcpsd <- gsub("[{]2[}]", temp[i, 'POLICYCD'], temcpsd)
        temcpsd <- gsub("[{]3[}]", temp[i, 'PRODUCTCD'], temcpsd)
        temcpsd <- gsub("[{]4[}]", temp[i, 'PRODNUNITCD'], temcpsd)
        print(temcpsd)
        baseondetail <- execute_sql(temcpsd, env)
        scp = baseondetail$PRODNSUMMSEQ
      }
      sample_cps2$PRODNSUMMSEQ = scp
      
      # save the new row to output file csv
      if (!(file.exists(cgbk.csv)))
        data.table::fwrite(sample_cps2, cgbk.csv, append = FALSE)
      else
        data.table::fwrite(sample_cps2, cgbk.csv, append = TRUE)
    }
  }
}
#
# GENERATING PATCHING SCRIPT ####
# 
if (file.exists(cps.csv)) {
  t1 <- data.table::fread(file = cps.csv, showProgress = TRUE)
  sqls <- vector(mode = 'character')
  for (i in 1:nrow(t1)) {
    values <- paste(select(t1, -c(SCOPE_IDENTITY, PRODNSUMMSEQ))[i], collapse = "','")
    sql1 <- "INSERT INTO CMS_PRODUCTION_SUMMARY (%s) VALUES ('%s')"
    sql1 <- sprintf(sql1, paste(colnames(select(t1, -c(SCOPE_IDENTITY, PRODNSUMMSEQ))), collapse = ','), values)
    sql1 <- gsub("'NA'", 'NULL', sql1)
    sql1 <- gsub("''", 'NULL', sql1)
    sql1 <- paste(sql1, t1[i, 'SCOPE_IDENTITY'], sep = '\n')
    sqls <- c(sqls, sql1)
  }
  save_to_sql_file(paste(sqls, collapse = '\n'), out_put_patched_file, append = TRUE)
}

if (file.exists(cpsd.csv)) {
  
}
  
if (file.exists(cgbk.csv)) {
  t1 <- data.table::fread(file = cgbk.csv, showProgress = TRUE)
  sqls <- vector(mode = 'character')
  for (i in 1:nrow(t1)) {
    values <- paste(t1[i], collapse = "','")
    sql1 <- "INSERT INTO CMS_GROUPPRODN_SUMM_BK (%s) VALUES ('%s')"
    sql1 <- sprintf(sql1, paste(colnames(t1), collapse = ','), values)
    sql1 <- gsub("'NA'", 'NULL', sql1)
    sql1 <- gsub("''", 'NULL', sql1)
    sqls <- c(sqls, sql1)
  }
  save_to_sql_file(paste(sqls, collapse = '\n'), out_put_patched_file, append = TRUE)
}

rm(list = ls())
