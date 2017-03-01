source('EDM_set_common_input.R')
#---- EXTRACT PAYABLE OF TIEDAGENCY ----
payables <- get_payable(env, '2017-02-01', '2017-02-28', 'TIEDAGENCY')

#---- EXTRACT PAYABLE OF BANCASSURANCE----
get_all_agencies(env)$CHANNELCD
payables <- get_payable(env, '2017-02-01', '2017-02-28', 'INHOUSE')
payout <- get_payout(env, '2017-03-06')
tax <- get_tax(env, '2017-02-28', 'INHOUSE')
#******DFU******####
#1__ set dfu id and expected file path ####
dfuid = 'DFUWF-756'
dfuifle = file.path(output, dfuid, 'DFU_Tax_Payout_20170228.xlsx')
#2__ read expected data from users ####
exp_payout <- readxl::read_excel(dfuifle, sheet = 'PAYOUT')
exp_tax <- readxl::read_excel(dfuifle, sheet = 'TAX')
#3__ generate update scripts ####
#__1__ update CMS_PAYOUT ####
select1 = "SELECT * FROM CMS_PAYOUT WHERE PAYOUTSEQ=%d"
update1 = "UPDATE CMS_PAYOUT SET TOTALAMT=%d WHERE PAYOUTSEQ=%d"
exp_payout_update <- dplyr::filter(exp_payout, !is.na(PAYOUTSEQ)) %>%
  dplyr::mutate(SELECT1 = sprintf(select1, PAYOUTSEQ)) %>%
  dplyr::mutate(UPDATE1 = sprintf(update1, TOOL_TOTALAMT, PAYOUTSEQ)) %>%
  print
#__2__ insert CMS_PAYOUT ####
# (can bo sung phan insert payout)####
exp_payout_insert <- dplyr::filter(exp_payout, is.na(PAYOUTSEQ))
producer<-get_producer_info(env, 'TM000002')
bankinfo <-get_producer_bankinfo(env, '2017-03-06', producer$CLIENTCD)
payto <- get_producer_payto(env, '2017-03-06', producer$PRODUCERCD)  

for (p in exp_payout_insert) {
  cms_payout <-
    dplyr::tbl_df(list(
      'TOTALAMT' = 199,
      'PAYOUTDT' = '2017-02-28',
      'NARRATION' = NA
    ))
}

#__3__ update CMS_TAX_PAYABLE ####
select1 = "SELECT * FROM CMS_TAX_PAYABLE WHERE TAXPAYABLESEQ=%d"
update1 = "UPDATE CMS_TAX_PAYABLE SET TAXAMOUNT=%d,TOTALTAXAMOUNT=%d,PAYABLEAMOUNT=%d,TOTALPAYABLEAMOUNT=%d WHERE TAXPAYABLESEQ=%d"
exp_tax_update <- dplyr::filter(exp_tax, !is.na(TAXPAYABLESEQ)) %>%
dplyr::mutate(SELECT1 = sprintf(select1, TAXPAYABLESEQ)) %>%
  dplyr::mutate(UPDATE1 = sprintf(update1, Tool_TAXAMOUNT, Tool_TAXAMOUNT, TOOL_PAYABLEAMOUNT, TOOL_TOTALPAYABLEAMOUNT, TAXPAYABLESEQ)) %>%
  print

#__4__ insert CMS_TAX_PAYABLE ####  
exp_tax_insert <- dplyr::filter(exp_tax, is.na(TAXPAYABLESEQ))
# (can bo sung phan insert tax payable)####


#4__ save dfu script ####
out_put_patched_file = file.path(output, dfuid, "script_patch.sql")
save_to_sql_file('', out_put_patched_file, append = FALSE) #replace all contents by nothing
save_to_sql_file(paste(exp_payout_update$SELECT1, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(exp_payout_update$UPDATE1, collapse = '; \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(exp_payout_update$SELECT1, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file('---------------------------------------------------------', out_put_patched_file, append = TRUE)
save_to_sql_file(paste(exp_tax_update$SELECT1, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(exp_tax_update$UPDATE1, collapse = '; \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(exp_tax_update$SELECT1, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)

