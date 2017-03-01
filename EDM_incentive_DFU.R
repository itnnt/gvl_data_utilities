source('EDM_set_common_input.R')
#---- dfu incentive -----
env = 'COMPANY_EDM_VN'
dfu = 'DFUWF-817'
checded_xlsx_file = 'IncentivesADHOC_Bancas_20170404.xlsx'
out_put_patched_file = file.path(output, dfu, paste(checded_xlsx_file, 'sql', sep = '.'))
#_ expected data from user----
incentive_from_user <-
  read.xlsx(file.path(output, dfu, checded_xlsx_file),
            sheetName = 1)
#_ format user's field names----
incentive_field_names <- names(incentive_from_user)
incentive_field_names <- gsub('BenefitCode', 'BENEFITCODE', incentive_field_names)
incentive_field_names <- gsub('KPICODE', 'BENEFITCODE', incentive_field_names)
incentive_field_names <- gsub('AGCode', 'AGENTCD', incentive_field_names)
incentive_field_names <- gsub('LEADERCD', 'AGENTCD', incentive_field_names)
incentive_field_names <- gsub('Agent.Code', 'AGENTCD', incentive_field_names)
incentive_field_names <- gsub('Agent Code', 'AGENTCD', incentive_field_names)
incentive_field_names <- gsub('TOO_Bonus', 'TOOL', incentive_field_names)
incentive_field_names <- gsub('GVL_Bonus', 'TOOL', incentive_field_names)
incentive_field_names <- gsub('EDM_Bonus', 'EDM', incentive_field_names)
incentive_field_names <- gsub('Check_Bonus', 'CHECK', incentive_field_names)
incentive_field_names <- gsub('EDM_Seq', 'BENEFITRESULTSEQ', incentive_field_names)
#_rename data frame
names(incentive_from_user) <- incentive_field_names
incentive_from_user <- dplyr::filter(incentive_from_user, !is.na(incentive_from_user$AGENTCD))

# corespondent data from edm
seq_incentive <- paste(incentive_from_user$BENEFITRESULTSEQ, collapse = ',')
incentive_from_edm <-
  get_incentive_result(env, 'CB.BENEFITRESULTSEQ' = sprintf("(%s)", seq_incentive))
seq_payable <- paste(incentive_from_edm$BENEFITPAYRESULTSEQ, collapse=',')
incentive_payable <- get_payable_tbl(env, "REFERENCEBASETYPECD"="('INCENTIVE')", 'REFERENCESEQ'=sprintf("(%s)", seq_payable))

incentive_from_user <- dplyr::tbl_df(incentive_from_user)
incentive_from_edm <- dplyr::tbl_df(incentive_from_edm)
incentive_payable <- dplyr::tbl_df(incentive_payable)

# left join
merged_incentive <- merge(
  x = incentive_from_user,
  y = incentive_from_edm,
  by.x = c("BENEFITRESULTSEQ" ),
  by.y = c("BENEFITRESULTSEQ" ),
  all.x = TRUE
)
merged_incentive$REFERENCEBASETYPECD='INCENTIVE'
# left join
merged_payable <- merge(
  x = merged_incentive,
  y = incentive_payable,
  by.x = c("BENEFITPAYRESULTSEQ", "REFERENCEBASETYPECD" ),
  by.y = c("REFERENCESEQ", "REFERENCEBASETYPECD" ),
  all.x = TRUE
)

select1 = "SELECT * FROM CMS_BENEFITRESULT WHERE BENEFITRESULTSEQ=%s"
update1 = "UPDATE CMS_BENEFITRESULT SET BENEFITAMT=%s WHERE BENEFITRESULTSEQ=%s"
select2 = "SELECT * FROM CMS_BENEFITPAY_RESULT WHERE BENEFITPAYRESULTSEQ=%s"
update2 = "UPDATE CMS_BENEFITPAY_RESULT SET BENEFITPAYAMT=%s WHERE BENEFITPAYRESULTSEQ=%s"
select3 = "SELECT * FROM CMS_PRODUCER_PAYABLE WHERE PRODUCERPAYABLESEQ=%s"
update3 = "UPDATE CMS_PRODUCER_PAYABLE SET TOTALPAYMENTAMT=%s, PAYMENTAMT=%s WHERE PRODUCERPAYABLESEQ=%s"

merged_incentive <- merged_incentive %>%
  dplyr::mutate(SELECT1 = ifelse(!is.na(BENEFITRESULTSEQ), sprintf(select1, BENEFITRESULTSEQ),'')) %>%
  dplyr::mutate(UPDATE1 = ifelse(!is.na(BENEFITRESULTSEQ), sprintf(update1, format(TOOL, scientific=FALSE), BENEFITRESULTSEQ), '')) %>%
  dplyr::mutate(SELECT2 = ifelse(!is.na(BENEFITPAYRESULTSEQ), sprintf(select2, BENEFITPAYRESULTSEQ), '')) %>%
  dplyr::mutate(UPDATE2 = ifelse(!is.na(BENEFITPAYRESULTSEQ), sprintf(update2, format(TOOL, scientific=FALSE), BENEFITPAYRESULTSEQ), '')) %>%
  print

merged_payable <- merged_payable %>%
  dplyr::mutate(SELECT3 = ifelse(!is.na(PRODUCERPAYABLESEQ), sprintf(select3, PRODUCERPAYABLESEQ), '')) %>%
  dplyr::mutate(UPDATE3 = ifelse(!is.na(PRODUCERPAYABLESEQ), sprintf(update3, format(TOOL, scientific=FALSE), format(TOOL, scientific=FALSE), PRODUCERPAYABLESEQ), '')) %>%
  print

save_to_sql_file('', out_put_patched_file, append = FALSE)
save_to_sql_file(paste(merged_incentive$SELECT1, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(merged_incentive$UPDATE1, collapse = '; \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(merged_incentive$SELECT1, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file('---------------------------------------------------------', out_put_patched_file, append = TRUE)
save_to_sql_file(paste(merged_incentive$SELECT2, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(merged_incentive$UPDATE2, collapse = '; \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(merged_incentive$SELECT2, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file('---------------------------------------------------------', out_put_patched_file, append = TRUE)
save_to_sql_file(paste(filter(merged_payable, merged_payable$SELECT3!='')$SELECT3, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(filter(merged_payable, merged_payable$UPDATE3!='')$UPDATE3, collapse = '; \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(filter(merged_payable, merged_payable$SELECT3!='')$SELECT3, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
