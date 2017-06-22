if (Sys.getenv("JAVA_HOME") != "") {
  Sys.setenv(JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_131")
}
source('EDM_set_common_input.R')

out_put_patched_file = file.path('mapping_account', paste('account_mapping', 'sql', sep = '.'))
#
# PREPARE PATCH FILE OUPUT
# 
save_to_sql_file('', out_put_patched_file, append = FALSE)
#
#
D <- "
insert into CMS_INT_ACCTCODEMAP_M_TEMP(PaymentType, ChannelCd, OfficeCd, ProductCd,Accountcd,Description, SubItem ,PartnerUnit, LoB,EffectiveFromDt,EffectiveTodt, CreatedBy, Createddt, Createdat, Updatedby,Updateddt, Updatedat, Rowversion, debitcredit, FundCD, YearinForceFrom,YearinForceTo, TCODE8)
values('%s',NULL,NULL,NULL,'%s',N'%s',null,'999999','ZZ','2013-01-01',null,'USER','2012-01-01','AA','USER',NULL,'AA','1','D','NPF',null,null,'N')
"
C <- "
insert into CMS_INT_ACCTCODEMAP_M_TEMP(PaymentType, ChannelCd, OfficeCd, ProductCd,Accountcd,Description, SubItem ,PartnerUnit, LoB,EffectiveFromDt,EffectiveTodt, CreatedBy, Createddt, Createdat, Updatedby,Updateddt, Updatedat, Rowversion, debitcredit, FundCD, YearinForceFrom,YearinForceTo)
values('%s',NULL,NULL,NULL,'%s',N'%s','L58','999999',null,'2013-01-01',null,'USER','2012-01-01','AA','USER',NULL,'AA','1','C',null,null,null)
"
DCLB <- "
insert into CMS_INT_ACCTCODEMAP_M_TEMP(PaymentType, ChannelCd, OfficeCd, ProductCd,Accountcd,Description, SubItem ,PartnerUnit, LoB,EffectiveFromDt,EffectiveTodt, CreatedBy, Createddt, Createdat, Updatedby,Updateddt, Updatedat, Rowversion, debitcredit, FundCD, YearinForceFrom,YearinForceTo, TCODE8)
values('%sCLB',NULL,NULL,NULL,'%s',N'%s',null ,'999999','ZZ','2013-01-01',null,'USER','2012-01-01','AA','USER',NULL,'AA','1','C','NPF',null,null,'N')
"
CCLB <- "
insert into CMS_INT_ACCTCODEMAP_M_TEMP(PaymentType, ChannelCd, OfficeCd, ProductCd,Accountcd,Description, SubItem ,PartnerUnit, LoB,EffectiveFromDt,EffectiveTodt, CreatedBy, Createddt, Createdat, Updatedby,Updateddt, Updatedat, Rowversion, debitcredit, FundCD, YearinForceFrom,YearinForceTo)
values('%sCLB',NULL,NULL,NULL,'%s',N'%s','L58','999999',null,'2013-01-01',null,'USER','2012-01-01','AA','USER',NULL,'AA','1','D',null,null,null)
"

new_mapping_acc <- read_excel("D:/workspace_r/gvl_data_utilities/mapping_account/Mapping_Account.xlsx")
# format columns' names
colnames <- names(new_mapping_acc)
colnames <- gsub("Incentive rule", "RULECD", colnames)
colnames <- gsub("\\<Description\\>", "DESCRIPTION", colnames)
colnames <- gsub("\\<EN_Description\\>", "DESCRIPTION", colnames)
colnames <- gsub("Dr", "D", colnames)
colnames <- gsub("Cr", "C", colnames)
names(new_mapping_acc) <- colnames

new_mapping_acc <- (tidyr::gather(new_mapping_acc, ACCOUNTTYPE, ACCOUNTNUM, -c(RULECD, DESCRIPTION)))
new_mapping_acc <- unique(new_mapping_acc) %>%
  mutate(SQL=ifelse(ACCOUNTTYPE=='D', sprintf(D, RULECD, ACCOUNTNUM, trim(DESCRIPTION)), ifelse(ACCOUNTTYPE=='C', sprintf(C, RULECD, ACCOUNTNUM, trim(DESCRIPTION)), ''))) %>% 
  mutate(SQLCLB=ifelse(ACCOUNTTYPE=='D', sprintf(DCLB, RULECD, ACCOUNTNUM, trim(DESCRIPTION)), ifelse(ACCOUNTTYPE=='C', sprintf(CCLB, RULECD, ACCOUNTNUM, trim(DESCRIPTION)), ''))) 

# save sql file supporting utf8
sql <- vector(mode = 'character')
sql <- c(sql, new_mapping_acc$SQL)
sql <- c(sql, new_mapping_acc$SQLCLB)
writeLines(sql, out_put_patched_file, useBytes=T)