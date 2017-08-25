source('KPI_PRODUCTION/Common functions.R')

exceltemplate = sprintf("d:\\workspace_r\\gvl_data_utilities\\KPI_PRODUCTION\\output\\Agent Retention\\Agency Retention 201706.xlsx", strftime(Sys.time(),'%y%m%d'))
excelresult = sprintf("d:\\workspace_r\\gvl_data_utilities\\KPI_PRODUCTION\\output\\Agent Retention\\Agency Retention 201707.xlsx", strftime(Sys.time(),'%y%m%d'))
# sheet North -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

df=report_agent_retention(criteria = "where value!=0 and value is not null")
df=tidyr::spread(df, time_view, value)
# set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
rownames(df)<-gsub("^\\s+|\\s+$", "", df[,'kpi'])

RDCOMClient_replace_cellvalues(
  df=df,
  excelfile=exceltemplate,
  newexcelfile = excelresult,
  sheetname="5.0 AG retention",
  visible=T,
  rowNameColIndex=1,
  headerRowIndex=1
)





