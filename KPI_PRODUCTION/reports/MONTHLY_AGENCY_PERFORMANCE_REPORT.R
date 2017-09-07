source('KPI_PRODUCTION/Common functions.R')

exceltemplate = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\Agency_Monthly_Report_template.xlsx", strftime(Sys.time(),'%Y%m%d'))
excelresult = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\Agency_Monthly_Report_%s.xlsx", strftime(Sys.time(),'%Y%m%d'))
# sheet North -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

df=report_agent_retention(criteria = "where value!=0 and value is not null")
df=tidyr::spread(df, time_view, value)
# set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
rownames(df)<-gsub("^\\s+|\\s+$", "", df[,'kpi'])


# Sheet: 1.1 Overrall (Territory) -----------------------------------------


RDCOMClient_replace_cellvalues(
  df=df,
  excelfile=exceltemplate,
  newexcelfile = excelresult,
  sheetname="1.1 Overrall (Territory)",
  visible=T,
  rowNameColIndex=1,
  headerRowIndex=1
)





