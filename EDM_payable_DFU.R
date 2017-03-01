source('EDM_set_common_input.R')

env = 'COMPANY_EDM_VN'
dfu = 'DFUWF-818'
checded_xlsx_file = 'DFU_Payable_IncorrectAgent_20170331.xlsx'
checded_xlsx_file2 = 'DFU_Payable_IncorrectChannel_20170331.xlsx'

out_put_patched_file = file.path(output, dfu, paste(checded_xlsx_file, 'sql', sep = '.'))
#_ expected data from user----
if (!is.na(checded_xlsx_file)) {
  payable_from_user <- read.xlsx(file.path(output, dfu, checded_xlsx_file), sheetName = 1)
  fieldnames <- names(payable_from_user)
  fieldnames <- gsub("\\<sourceproducercd\\>", "SOURCEPRODUCERCD", fieldnames)
  fieldnames <- gsub("\\<producerpayableseq\\>", "PRODUCERPAYABLESEQ", fieldnames)
  fieldnames <- gsub("\\<tool\\>", "TOOL", fieldnames)
  fieldnames <- gsub("\\<edm\\>", "EDM", fieldnames)
  names(payable_from_user) <- fieldnames
  payable_from_user$TYPE = 'AGENT'
}

if (!is.na(checded_xlsx_file2)) {
  payable_from_user2 <- read.xlsx(file.path(output, dfu, checded_xlsx_file2), sheetName = 1)
  fieldnames <- names(payable_from_user2)
  fieldnames <- gsub("\\<sourceproducercd\\>", "SOURCEPRODUCERCD", fieldnames)
  fieldnames <- gsub("\\<producerpayableseq\\>", "PRODUCERPAYABLESEQ", fieldnames)
  fieldnames <- gsub("\\<tool\\>", "TOOL", fieldnames)
  fieldnames <- gsub("\\<edm\\>", "EDM", fieldnames)
  names(payable_from_user2) <- fieldnames
  payable_from_user2$TYPE = 'CHANNEL'
}

if (exists('payable_from_user') & exists('payable_from_user2')) {
  checkedall <- rbind(payable_from_user, payable_from_user2)
} else if (exists('payable_from_user')) {
  checkedall <- payable_from_user
} else if (exists('payable_from_user2')) {
  checkedall <- payable_from_user2
}

if (exists('checkedall')) {
  mutate(checkedall, checkedall$UPDATE <- ifelse(TYPE=='AGENT', sprintf("UPDATE CMS_PRODUCER_PAYABLE SET PAYTOPRODUCERCD=(SELECT PRODUCERCD FROM CMS_PRODUCERCHANNEL_M WHERE PRODUCERSOURCECD='%s') WHERE PRODUCERPAYABLESEQ=%s;", TOOL, format(PRODUCERPAYABLESEQ, scientific = F)),
                                       ifelse(TYPE=='CHANNEL', sprintf("UPDATE CMS_PRODUCER_PAYABLE SET CHANNELCD='%s' WHERE PRODUCERPAYABLESEQ=%s;", TOOL, format(PRODUCERPAYABLESEQ, scientific = F)),'')))

  mutate(checkedall, checkedall$SELECT <- sprintf("SELECT * FROM CMS_PRODUCER_PAYABLE WHERE PRODUCERPAYABLESEQ=%s", format(PRODUCERPAYABLESEQ, scientific = F)))  
}

save_to_sql_file('', out_put_patched_file, append = FALSE)
save_to_sql_file(paste(checkedall$SELECT, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(checkedall$UPDATE, collapse = ' \n'), out_put_patched_file, append = TRUE)
save_to_sql_file(paste(checkedall$SELECT, collapse = ' UNION ALL \n'), out_put_patched_file, append = TRUE)



