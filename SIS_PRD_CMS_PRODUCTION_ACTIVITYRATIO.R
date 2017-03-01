source("EDM_set_common_input.R")
sqlText <- "
select 
a.PRODUCERCD,
a.PRODNUNITCD,
a.productcd, 
a.prodnvalue, 
a.PROCESSDT,
a.PRODNTYPECD,
a.PRODNGROUPCD,
a.TEAMCD,
a.UNITBRANCHCD, 
a.UNITBRANCHNAME,
a.DESIGNATIONCD,
a.PRODUCERSOURCECD, 
a.PREFERREDNAME,
a.CHANNELCD
from CMS_PRODUCTION_ACTIVITYRATIO a
where  
a.processdt in ('2017-01-31','2017-02-28','2017-03-19') 
and a.PRODNUNITCD in (N'ACTIVEAGTWTEXCL', N'AGTCNTATMNTHSTART', N'AGTCNTNEWRECRUIT', N'AGTCNTREINSTATE', N'AGTCNTTERMINATED', N'AGTCNTATMNTHEND')
and (a.PRODNGROUPCD in ('USDEF4','UMDEF1','BMDEF1','BMDEF7') or a.PRODNGROUPCD like '%TEAM%')
"

dir.create(file.path(output, 'CMS_PRODUCTION_ACTIVITYRATIO'), showWarnings = FALSE)

production_activity_ratio <- execute_sql(sqlText, 'SIS_PRD')
# convert yyyy-mm-dd string to date
production_activity_ratio$PROCESSDT <- as.Date(production_activity_ratio$PROCESSDT, "%Y-%m-%d")
# reformat date to dd-mm-yyyy
production_activity_ratio$PROCESSDT <- format(production_activity_ratio$PROCESSDT, "%d-%m-%Y")
# write.xlsx(production_activity_ratio, file.path(output, 'CMS_PRODUCTION_ACTIVITYRATIO', 'detail.xlsx'))

data.table::fwrite(production_activity_ratio, file.path(output, 'CMS_PRODUCTION_ACTIVITYRATIO', 'detail.csv'))


head(production_activity_ratio)
filter(production_activity_ratio, UNITBRANCHCD=='G1023', PRODNUNITCD=='AGTCNTATMNTHSTART')
