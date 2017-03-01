library(readxl)
source('util/util_data_access_sqlite.R')
source('util/util_extract_data.R')

# set output folder ####
output = 'S:/IT/Share to others/tung'

# set input parameters ####
defectid = 'AAR-96'
checkedresult = 'IncomeStatement_20170530_banca_Check.xlsx'
income_y = 2017
income_m = 5
income_h = 2

# input expected data ####
if (!exists('payment_status'))
{
  payment_status <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 1)
  names(payment_status) <- trimws(names(payment_status))
  names(payment_status) <- gsub('Agent code', 'AGENTCD', names(payment_status))
  names(payment_status) <- gsub('Agent Code', 'AGENTCD', names(payment_status))
  names(payment_status) <- gsub('AA', 'EXPECTED', names(payment_status))
  payment_status <- dplyr::filter(payment_status, !is.na(AGENTCD))
}

if (!exists('ytd_tax'))
{
  ytd_tax <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 2) 
  names(ytd_tax) <- trimws(names(ytd_tax))
  names(ytd_tax) <- gsub('Agent', 'AGENTCD', names(ytd_tax))
  names(ytd_tax) <- gsub('AA', 'EXPECTED', names(ytd_tax))
  names(ytd_tax) <- gsub('Check', 'CHECK', names(ytd_tax))
  ytd_tax$CHECK <- format(ytd_tax$CHECK, scientific=FALSE)
  ytd_tax <- dplyr::filter(ytd_tax, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('netincome'))
{
  netincome <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 3) 
  names(netincome) <- trimws(names(netincome))
  names(netincome) <- gsub('Agent', 'AGENTCD', names(netincome))
  names(netincome) <- gsub('AG', 'AGENTCD', names(netincome))
  names(netincome) <- gsub('AA', 'EXPECTED', names(netincome))
  names(netincome) <- gsub('Check', 'CHECK', names(netincome))
  netincome$CHECK <- format(netincome$CHECK, scientific=FALSE)
  netincome <- dplyr::filter(netincome, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('tax'))
{
  tax <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 4) 
  names(tax) <- trimws(names(tax))
  names(tax) <- gsub('AG', 'AGENTCD', names(tax))
  names(tax) <- gsub('AA', 'EXPECTED', names(tax))
  names(tax) <- gsub('Check', 'CHECK', names(tax))
  tax$CHECK <- format(tax$CHECK, scientific=FALSE)
  tax <- dplyr::filter(tax, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('immediatepaid'))
{
  immediatepaid <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 5) 
  names(immediatepaid) <- trimws(names(immediatepaid))
  names(immediatepaid) <- gsub('Agent code', 'AGENTCD', names(immediatepaid))
  names(immediatepaid) <- gsub('AA', 'EXPECTED', names(immediatepaid))
  names(immediatepaid) <- gsub('Check', 'CHECK', names(immediatepaid))
  immediatepaid$CHECK <- format(immediatepaid$CHECK, scientific=FALSE)
  immediatepaid <- dplyr::filter(immediatepaid, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('incentive'))
{
  incentive <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 6) 
  names(incentive) <- trimws(names(incentive))
  names(incentive) <- gsub('Agent code', 'AGENTCD', names(incentive))
  names(incentive) <- gsub('AA', 'EXPECTED', names(incentive))
  names(incentive) <- gsub('Check', 'CHECK', names(incentive))
  incentive$CHECK <- format(incentive$CHECK, scientific=FALSE)
  incentive <- dplyr::filter(incentive, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('ytd_income'))
{
  ytd_income <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 7) 
  names(ytd_income) <- trimws(names(ytd_income))
  names(ytd_income) <- gsub('Agent code', 'AGENTCD', names(ytd_income))
  names(ytd_income) <- gsub('AgentCD', 'AGENTCD', names(ytd_income))
  names(ytd_income) <- gsub('AA', 'EXPECTED', names(ytd_income))
  names(ytd_income) <- gsub('Check', 'CHECK', names(ytd_income))
  ytd_income$CHECK <- format(ytd_income$CHECK, scientific=FALSE)
  ytd_income <- dplyr::filter(ytd_income, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('premonthholdincome'))
{
  premonthholdincome <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 8) 
  names(premonthholdincome) <- trimws(names(premonthholdincome))
  names(premonthholdincome) <- gsub('Agent code', 'AGENTCD', names(premonthholdincome))
  names(premonthholdincome) <- gsub('AgentCD', 'AGENTCD', names(premonthholdincome))
  names(premonthholdincome) <- gsub('AA', 'EXPECTED', names(premonthholdincome))
  names(premonthholdincome) <- gsub('Check', 'CHECK', names(premonthholdincome))
  premonthholdincome$CHECK <- format(premonthholdincome$CHECK, scientific=FALSE)
  premonthholdincome <- dplyr::filter(premonthholdincome, !is.na(AGENTCD), !is.na(CHECK))
}
if (!exists('rejectFromBank'))
{
  rejectFromBank <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 9) 
  names(rejectFromBank) <- trimws(names(rejectFromBank))
  names(rejectFromBank) <- gsub('Agent code', 'AGENTCD', names(rejectFromBank))
  names(rejectFromBank) <- gsub('AgentCD', 'AGENTCD', names(rejectFromBank))
  names(rejectFromBank) <- gsub('AA', 'EXPECTED', names(rejectFromBank))
  names(rejectFromBank) <- gsub('Check', 'CHECK', names(rejectFromBank))
  rejectFromBank$CHECK <- format(rejectFromBank$CHECK, scientific=FALSE)
  rejectFromBank <- dplyr::filter(rejectFromBank, !is.na(AGENTCD), !is.na(CHECK))
}
if (!exists('postTaxAdjustment'))
{
  postTaxAdjustment <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 10) 
  names(postTaxAdjustment) <- trimws(names(postTaxAdjustment))
  names(postTaxAdjustment) <- gsub('Agent code', 'AGENTCD', names(postTaxAdjustment))
  names(postTaxAdjustment) <- gsub('AG', 'AGENTCD', names(postTaxAdjustment))
  names(postTaxAdjustment) <- gsub('AgentCD', 'AGENTCD', names(postTaxAdjustment))
  names(postTaxAdjustment) <- gsub('AA', 'EXPECTED', names(postTaxAdjustment))
  names(postTaxAdjustment) <- gsub('Check', 'CHECK', names(postTaxAdjustment))
  postTaxAdjustment$CHECK <- format(postTaxAdjustment$CHECK, scientific=FALSE)
  postTaxAdjustment <- dplyr::filter(postTaxAdjustment, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('taxAdjustment'))
{
  taxAdjustment <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 11) 
  names(taxAdjustment) <- trimws(names(taxAdjustment))
  names(taxAdjustment) <- gsub('AG', 'AGENTCD', names(taxAdjustment))
  names(taxAdjustment) <- gsub('AA', 'EXPECTED', names(taxAdjustment))
  names(taxAdjustment) <- gsub('Check', 'CHECK', names(taxAdjustment))
  taxAdjustment$CHECK <- format(taxAdjustment$CHECK, scientific=FALSE)
  taxAdjustment <- dplyr::filter(taxAdjustment, !is.na(AGENTCD), !is.na(CHECK))
}

if (!exists('incentive_nextmth'))
{
  incentive_nextmth <-
    readxl::read_excel(file.path(output, defectid, checkedresult), sheet = 12) 
  names(incentive_nextmth) <- trimws(names(incentive_nextmth))
  names(incentive_nextmth) <- gsub('Agent code', 'AGENTCD', names(incentive_nextmth))
  names(incentive_nextmth) <- gsub('AG', 'AGENTCD', names(incentive_nextmth))
  names(incentive_nextmth) <- gsub('AA', 'EXPECTED', names(incentive_nextmth))
  names(incentive_nextmth) <- gsub('Check', 'CHECK', names(incentive_nextmth))
  incentive_nextmth$CHECK <- format(incentive_nextmth$CHECK, scientific=FALSE)
  incentive_nextmth <- dplyr::filter(incentive_nextmth, !is.na(AGENTCD), !is.na(CHECK))
}

#_ reset output file ####
dbfile = 'D:/workspace_incomestatement/data.db'
out_put_patched_file = file.path(output, defectid, "filename.sql")
save_to_sql_file('', out_put_patched_file, append = FALSE)

#_ patch incentive ####
if (nrow(incentive))
for (i in 1:nrow(incentive)) {
  st <- get_agent_income(income_y, income_m, income_h, incentive[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, incentive[i, 'AGENTCD'])
  setvalues <- gsub("0", incentive[i, 'CHECK'], "SET incentive=incentive+ 0, total_pretax_income=total_pretax_income+ 0, ytd_pretax_income=ytd_pretax_income+ 0")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE)
}

#_ patch incentive_nextmth ####
if (nrow(incentive_nextmth))
for (i in 1:nrow(incentive_nextmth)) {
  st <- get_agent_income(income_y, income_m, income_h, incentive_nextmth[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, incentive_nextmth[i, 'AGENTCD'])
  setvalues <- gsub("0", incentive_nextmth[i, 'CHECK'], "SET incentive_nextmth=incentive_nextmth+ 0, total_pretax_income_nextmth=total_pretax_income_nextmth+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE)
}

#_ patch payment status ####
if (nrow(payment_status))
for (i in 1:nrow(payment_status)) {
  st <- get_agent_income(income_y, income_m, income_h, payment_status[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, payment_status[i, 'AGENTCD'])
  if (payment_status[i, 'EXPECTED'] == 'I') {
    setvalues <- "SET note='Thu nhập chưa được chuyển khoản do thấp hơn mức qui định (200.000 VNĐ)', paidincome=0"
  } else if (payment_status[i, 'EXPECTED'] == 'H') {
    setvalues <- "SET note='Tạm giữ thu nhập', paidincome=0"
  }
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE)
}

#_ patch ytd tax ####
if (nrow(ytd_tax))
for (i in 1:nrow(ytd_tax)) {
  st <- get_agent_income(income_y, income_m, income_h, ytd_tax[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, ytd_tax[i, 'AGENTCD'])
  setvalues <- gsub("0", ytd_tax[i, 'CHECK'], "SET ytd_tax=ytd_tax+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

#_ patch immediatepaid ####
if (nrow(immediatepaid))
for (i in 1:nrow(immediatepaid)) {
  st <- get_agent_income(income_y, income_m, income_h, immediatepaid[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, immediatepaid[i, 'AGENTCD'])
  setvalues <- gsub("0", immediatepaid[i, 'CHECK'], "SET immediate_payment=immediate_payment+ 0, total_posttax_income=total_posttax_income- 0, ytd_pretax_income=ytd_pretax_income- 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

#_ patch premonthholdincome ####
if (nrow(premonthholdincome))
for (i in 1:nrow(premonthholdincome)) {
  st <- get_agent_income(income_y, income_m, income_h, premonthholdincome[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, premonthholdincome[i, 'AGENTCD'])
  setvalues <- gsub("0", premonthholdincome[i, 'CHECK'], "SET premth_hold_income=premth_hold_income+ 0, total_posttax_income=total_posttax_income+ 0, ytd_pretax_income=ytd_pretax_income+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

#_ patch rejectFromBank ####
if (nrow(rejectFromBank))
for (i in 1:nrow(rejectFromBank)) {
  st <- get_agent_income(income_y, income_m, income_h, rejectFromBank[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, rejectFromBank[i, 'AGENTCD'])
  setvalues <- gsub("0", rejectFromBank[i, 'CHECK'], "SET rejectFromBank=rejectFromBank+ 0, total_posttax_income=total_posttax_income+ 0, ytd_pretax_income=ytd_pretax_income+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

#_ patch postTaxAdjustment ####
if (nrow(postTaxAdjustment))
for (i in 1:nrow(postTaxAdjustment)) {
  st <- get_agent_income(income_y, income_m, income_h, postTaxAdjustment[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, postTaxAdjustment[i, 'AGENTCD'])
  setvalues <- gsub("0", postTaxAdjustment[i, 'CHECK'], "SET postTaxAdjustment=postTaxAdjustment+ 0, total_posttax_income=total_posttax_income+ 0, ytd_pretax_income=ytd_pretax_income+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

#_ patch netincome ####
if (nrow(netincome))
for (i in 1:nrow(netincome)) {
  st <- get_agent_income(income_y, income_m, income_h, netincome[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, netincome[i, 'AGENTCD'])
  setvalues <- ''
  if (netincome[i, 'EXPECTED'] >= 200000) {
    setvalues <- 'SET netincome=netincome+ %, paidincome=paidincome+ %'
  } else {
    setvalues <- 'SET netincome=netincome+ %, paidincome=0'
  }
  setvalues <- gsub("%", netincome[i, 'CHECK'], setvalues)
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

#_ patch tax ####
if (nrow(tax))
for (i in 1:nrow(tax)) {
  st <- get_agent_income(income_y, income_m, income_h, tax[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, tax[i, 'AGENTCD'])
  setvalues <- gsub("0", tax[i, 'CHECK'], "SET tax=tax+ 0, total_tax=total_tax+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}
#_ patch taxAdjustment ####
if (nrow(taxAdjustment))
for (i in 1:nrow(taxAdjustment)) {
  st <- get_agent_income(income_y, income_m, income_h, taxAdjustment[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, tax[i, 'AGENTCD'])
  setvalues <- gsub("0", taxAdjustment[i, 'CHECK'], "SET taxAdjustment=taxAdjustment+ 0, total_tax=total_tax+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

#_ patch ytd_income ####
if (nrow(ytd_income))
for (i in 1:nrow(ytd_income)) {
  st <- get_agent_income(income_y, income_m, income_h, ytd_income[i, 'AGENTCD'], dbfile)
  sql_update <- sprintf("UPDATE summary_income_4JasperPrinting {0} WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s AND agent_code='%s'", income_y, income_m, income_h, ytd_income[i, 'AGENTCD'])
  setvalues <- gsub("0", ytd_income[i, 'CHECK'], "SET ytd_pretax_income=ytd_pretax_income+ 0 ")
  sql_update <- gsub("[{][0][}]", setvalues, sql_update)
  st$UPDATE <- sql_update
  save_to_sql_file(paste(st$UPDATE, collapse = '; \n'), out_put_patched_file, append = TRUE) 
}

rm(list=ls())
