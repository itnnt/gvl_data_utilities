source('KPI_PRODUCTION/Common functions.R')

excelFile = sprintf("KPI_PRODUCTION/output/Rookie_performance_%s.xlsx", strftime(Sys.time(),'%y%m%d'))
# sheet North -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

df=report_rookie_metric_by_recruited_month(criteria = "where yy>=2016 and value!=0")
df=tidyr::spread(df, time_view, value)
# set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
rownames(df)<-gsub("^\\s+|\\s+$", "", df[,'kpi'])

replace_cellvalue2.1(df,
                     sheetname="MetricbyRecruitedMonth",
                     rowNameColIndex = 1,
                     headerRowIndex = 1,
                     template="KPI_PRODUCTION/output/Rookie performance/Rookie_performance_template.xlsx",
                     result_file=excelFile)




