source('EDM_set_common_input.R')

sql1<-
"
select * from cms_production_summary
where processdt='2017-02-28' and producercd in (
select producercd from cms_producerchannel_m where CHANNELCD IN ( 'INHOUSE', 'BANKSTAFF')
)
"

sql2<-
  "
select * from CMS_PROD_SUMMARY_DTL where PRODNSUMMSEQ in (
  select PRODNSUMMSEQ from cms_production_summary
where processdt='2017-02-28' and producercd in (
 select producercd from cms_producerchannel_m where CHANNELCD IN ( 'INHOUSE', 'BANKSTAFF')
)
)
"

sql3<-
  "
select * from CMS_GROUPPRODN_SUMM_BK
where processdt='2017-02-28' and producercd in (
 select producercd from cms_producerchannel_m where CHANNELCD IN ( 'INHOUSE', 'BANKSTAFF')
)
"
CMS_PRODUCTION_SUMMARY <- execute_sql(sql1, 'COMPANY_EDM_VN_DR')
CMS_PROD_SUMMARY_DTL <- execute_sql(sql2, 'COMPANY_EDM_VN_DR')
CMS_GROUPPRODN_SUMM_BK <- execute_sql(sql3, 'COMPANY_EDM_VN_DR')

sequence = paste('@NEWSEQ', CMS_PRODUCTION_SUMMARY$PRODNSUMMSEQ, sep='_')
sequence = paste("DECLARE", sequence, 'NUMERIC(30,0)=SCOPE_IDENTITY();', sep=' ')

CMS_PRODUCTION_SUMMARY <- dplyr::mutate(CMS_PRODUCTION_SUMMARY, SCOPE_IDENTITY=sequence)

# CREATE FILE NAMES FOR OUTPUT ####
cps.csv = file.path(getwd(), 'EDM', paste('cps', 'csv', sep = '.'))
cpsd.csv = file.path(getwd(), 'EDM', paste('cpsd', 'csv', sep = '.'))
cgbk.csv = file.path(getwd(), 'EDM', paste('cgbk', 'csv', sep = '.'))
out_put_patched_file_CMS_PROD_SUMMARY_DTL = file.path(getwd(), 'EDM', paste('OUTPUT_SCRIPT__CMS_PROD_SUMMARY_DTL', 'sql', sep = '.'))
out_put_patched_file_CMS_GROUPPRODN_SUMM_BK = file.path(getwd(), 'EDM', paste('OUTPUT_SCRIPT__CMS_GROUPPRODN_SUMM_BK', 'sql', sep = '.'))
out_put_patched_file_CMS_PRODUCTION_SUMMARY1 = file.path(getwd(), 'EDM', paste('OUTPUT_SCRIPT__CMS_PRODUCTION_SUMMARY1', 'sql', sep = '.'))
out_put_patched_file_CMS_PRODUCTION_SUMMARY2 = file.path(getwd(), 'EDM', paste('OUTPUT_SCRIPT__CMS_PRODUCTION_SUMMARY2', 'sql', sep = '.'))
out_put_patched_file_CMS_PRODUCTION_SUMMARY3 = file.path(getwd(), 'EDM', paste('OUTPUT_SCRIPT__CMS_PRODUCTION_SUMMARY3', 'sql', sep = '.'))
out_put_patched_file_CMS_PRODUCTION_SUMMARY4 = file.path(getwd(), 'EDM', paste('OUTPUT_SCRIPT__CMS_PRODUCTION_SUMMARY4', 'sql', sep = '.'))
out_put_patched_file_CMS_PRODUCTION_SUMMARY5 = file.path(getwd(), 'EDM', paste('OUTPUT_SCRIPT__CMS_PRODUCTION_SUMMARY5', 'sql', sep = '.'))

# SAVE DATA TO OUTPUT FILES ####
if (!file.exists(cps.csv))
  data.table::fwrite(CMS_PRODUCTION_SUMMARY, cps.csv, append = FALSE)
if (!file.exists(cpsd.csv))
  data.table::fwrite(CMS_PROD_SUMMARY_DTL, cpsd.csv, append = FALSE)
if (!file.exists(cgbk.csv))
  data.table::fwrite(CMS_GROUPPRODN_SUMM_BK, cgbk.csv, append = FALSE)

# MAIN PROCESS ####
CMS_PRODUCTION_SUMMARY <- data.table::fread(file = cps.csv, showProgress = TRUE)
CMS_PROD_SUMMARY_DTL <- data.table::fread(file = cpsd.csv, showProgress = TRUE)
CMS_GROUPPRODN_SUMM_BK <- data.table::fread(file = cgbk.csv, showProgress = TRUE)

# temp_vector <- vector(mode='character')
# for (i in 1:nrow(CMS_PRODUCTION_SUMMARY)) {
#   temp_vector <- c(temp_vector, paste(CMS_PRODUCTION_SUMMARY[i,], collapse = ','))
# }
#-------------------
CMS_PRODUCTION_SUMMARY$OLDPRODNSUMMSEQ <- CMS_PRODUCTION_SUMMARY$PRODNSUMMSEQ
CMS_PRODUCTION_SUMMARY$PRODNSUMMSEQ <- (623737476+1):(623737476 + nrow(CMS_PRODUCTION_SUMMARY))
fieldnames <- paste(names(dplyr::select(CMS_PRODUCTION_SUMMARY, -c(OLDPRODNSUMMSEQ, SCOPE_IDENTITY))), collapse = ',')
CMS_PRODUCTION_SUMMARY$SQL <- apply(dplyr::select(CMS_PRODUCTION_SUMMARY, -c(OLDPRODNSUMMSEQ, SCOPE_IDENTITY))
                                    , 1
                                    , FUN = function(x) {
                                      x = gsub("^\\s+|\\s+$", "", x) # trim leading and trailing whitespace 
                                      val = gsub("'NA'", 'NULL', paste(x, collapse = "','")) # replace 'NA' by 'NULL'
                                      val = gsub("''", 'NULL', val) # replace '' by 'NULL'
                                      sprintf("INSERT INTO CMS_PRODUCTION_SUMMARY (%s) VALUES ('%s');", fieldnames, val)
                                    }
                                    )
a1 = CMS_PRODUCTION_SUMMARY$SQL[1:(nrow(CMS_PRODUCTION_SUMMARY)/5)]
a2 = CMS_PRODUCTION_SUMMARY$SQL[(length(a1)+1):(length(a1) + nrow(CMS_PRODUCTION_SUMMARY)/5)]
a3 = CMS_PRODUCTION_SUMMARY$SQL[(length(a1)+length(a2)+1):(length(a1)+length(a2)+nrow(CMS_PRODUCTION_SUMMARY)/5)]
a4 = CMS_PRODUCTION_SUMMARY$SQL[(length(a1)+length(a2)+length(a3)+1):(length(a1)+length(a2)+length(a3)+nrow(CMS_PRODUCTION_SUMMARY)/5)]
a5 = CMS_PRODUCTION_SUMMARY$SQL[(length(a1)+length(a2)+length(a3)+length(a4)+1):(length(a1)+length(a2)+length(a3)+length(a4)+nrow(CMS_PRODUCTION_SUMMARY)/5)]

save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY ON', out_put_patched_file_CMS_PRODUCTION_SUMMARY1, append = FALSE)
save_to_sql_file(paste(a1, collapse = '\n'), out_put_patched_file_CMS_PRODUCTION_SUMMARY1, append = TRUE)
save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY OFF', out_put_patched_file_CMS_PRODUCTION_SUMMARY1, append = TRUE)

save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY ON', out_put_patched_file_CMS_PRODUCTION_SUMMARY2, append = FALSE)
save_to_sql_file(paste(a2, collapse = '\n'), out_put_patched_file_CMS_PRODUCTION_SUMMARY2, append = TRUE)
save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY OFF', out_put_patched_file_CMS_PRODUCTION_SUMMARY2, append = TRUE)

save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY ON', out_put_patched_file_CMS_PRODUCTION_SUMMARY3, append = FALSE)
save_to_sql_file(paste(a3, collapse = '\n'), out_put_patched_file_CMS_PRODUCTION_SUMMARY3, append = TRUE)
save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY OFF', out_put_patched_file_CMS_PRODUCTION_SUMMARY3, append = TRUE)

save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY ON', out_put_patched_file_CMS_PRODUCTION_SUMMARY4, append = FALSE)
save_to_sql_file(paste(a4, collapse = '\n'), out_put_patched_file_CMS_PRODUCTION_SUMMARY4, append = TRUE)
save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY OFF', out_put_patched_file_CMS_PRODUCTION_SUMMARY4, append = TRUE)

save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY ON', out_put_patched_file_CMS_PRODUCTION_SUMMARY5, append = FALSE)
save_to_sql_file(paste(a5, collapse = '\n'), out_put_patched_file_CMS_PRODUCTION_SUMMARY5, append = TRUE)
save_to_sql_file('SET IDENTITY_INSERT CMS_PRODUCTION_SUMMARY OFF', out_put_patched_file_CMS_PRODUCTION_SUMMARY5, append = TRUE)

#-------------------
CMS_PROD_SUMMARY_DTL$OLDPRODNSUMMSEQ = CMS_PROD_SUMMARY_DTL$PRODNSUMMSEQ
# left join
merged_CMS_PROD_SUMMARY_DTL <- merge(
  x = dplyr::select(CMS_PROD_SUMMARY_DTL, -c(PRODNSUMMSEQ)),
  y = dplyr::select(CMS_PRODUCTION_SUMMARY, c(OLDPRODNSUMMSEQ,PRODNSUMMSEQ)),
  by.x = c("OLDPRODNSUMMSEQ" ),
  by.y = c("OLDPRODNSUMMSEQ" ),
  all.x = TRUE
)
fieldnames <- paste(names(dplyr::select(merged_CMS_PROD_SUMMARY_DTL, -c(PRODNSUMMDTLSEQ, OLDPRODNSUMMSEQ))), collapse = ',')
merged_CMS_PROD_SUMMARY_DTL$VAL <- apply(dplyr::select(merged_CMS_PROD_SUMMARY_DTL, -c(PRODNSUMMDTLSEQ, PRODNSUMMSEQ, OLDPRODNSUMMSEQ))
                                    , 1
                                    , FUN = function(x) {
                                      x = gsub("^\\s+|\\s+$", "", x) # trim leading and trailing whitespace 
                                      val = gsub("'NA'", 'NULL', paste(x, collapse = "','")) # replace 'NA' by 'NULL'
                                      val = gsub("''", 'NULL', val) # replace '' by 'NULL'
                                      sprintf("'%s'", val)
                                    }
)
merged_CMS_PROD_SUMMARY_DTL$SQL <- apply(dplyr::select(merged_CMS_PROD_SUMMARY_DTL, c(VAL, PRODNSUMMSEQ)) 
                                  , 1
                                  , FUN = function(x) {
                                    val = paste(x, collapse = ",")
                                    sprintf("INSERT INTO CMS_PROD_SUMMARY_DTL (%s) VALUES (%s);", fieldnames, val)
                                  })
save_to_sql_file('', out_put_patched_file_CMS_PROD_SUMMARY_DTL, append = FALSE)
save_to_sql_file(paste(merged_CMS_PROD_SUMMARY_DTL$SQL, collapse = '\n'), out_put_patched_file_CMS_PROD_SUMMARY_DTL, append = TRUE)
#-------------------
CMS_GROUPPRODN_SUMM_BK$OLDPRODNSUMMSEQ = CMS_GROUPPRODN_SUMM_BK$PRODNSUMMSEQ 
# left join
merged_CMS_GROUPPRODN_SUMM_BK <- merge(
  x = dplyr::select(CMS_GROUPPRODN_SUMM_BK, -c(PRODNSUMMSEQ)),
  y = dplyr::select(CMS_PRODUCTION_SUMMARY, c(OLDPRODNSUMMSEQ,PRODNSUMMSEQ)),
  by.x = c("OLDPRODNSUMMSEQ" ),
  by.y = c("OLDPRODNSUMMSEQ" ),
  all.x = TRUE
)
fieldnames <- paste(names(dplyr::select(merged_CMS_GROUPPRODN_SUMM_BK, -c(GRPPRODNSUMMBKSEQ, OLDPRODNSUMMSEQ))), collapse = ',')
merged_CMS_GROUPPRODN_SUMM_BK$VAL <- apply(dplyr::select(merged_CMS_GROUPPRODN_SUMM_BK, -c(GRPPRODNSUMMBKSEQ, PRODNSUMMSEQ, OLDPRODNSUMMSEQ))
                                  , 1
                                  , FUN = function(x) {
                                    x = gsub("^\\s+|\\s+$", "", x) # trim leading and trailing whitespace 
                                    val = gsub("'NA'", 'NULL', paste(x, collapse = "','")) # replace 'NA' by 'NULL'
                                    val = gsub("''", 'NULL', val) # replace '' by 'NULL'
                                    sprintf("'%s'", val)
                                  }
)
merged_CMS_GROUPPRODN_SUMM_BK$SQL <- apply(dplyr::select(merged_CMS_GROUPPRODN_SUMM_BK, c(VAL,PRODNSUMMSEQ)) 
                                  , 1
                                  , FUN = function(x) {
                                    val = paste(x, collapse = ",")
                                    sprintf("INSERT INTO CMS_GROUPPRODN_SUMM_BK (%s) VALUES (%s);", fieldnames, val)
                                  })
save_to_sql_file('', out_put_patched_file_CMS_GROUPPRODN_SUMM_BK, append = FALSE)
save_to_sql_file(paste(merged_CMS_GROUPPRODN_SUMM_BK$SQL, collapse = '\n'), out_put_patched_file_CMS_GROUPPRODN_SUMM_BK, append = TRUE)
