source('KPI_PRODUCTION/Common functions.R')

create_report <- function (bssdt, genlion_final_dt, exceltemplate, excelFile){
  # Sheet: Cover
  sheetname = 'Cover'
  cell_reporting_period = c(rowIndex=4, colIndex=5)
  fill_excel_cell(bssdt, 
                  sheetname, 
                  cell_reporting_period['rowIndex'], 
                  cell_reporting_period['colIndex'], 
                  exceltemplate, excelFile)
  
  # Sheet: Data
  sheetname = 'Data'
  replace_cellvalue2.1(ape_country(strftime(bssdt, '%Y'), dbfile),
                       sheetname=sheetname,
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  replace_cellvalue2.1(ape_territory(strftime(bssdt, '%Y'), dbfile),
                       sheetname=sheetname,
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  replace_cellvalue2.1(active_territory(strftime(bssdt, '%Y'), dbfile),
                       sheetname=sheetname,
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  replace_cellvalue2.1(case_territory(strftime(bssdt, '%Y'), dbfile),
                       sheetname=sheetname,
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  replace_cellvalue2.1(casesize_territory(strftime(bssdt, '%Y'), dbfile),
                       sheetname=sheetname,
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  replace_cellvalue2.1(fyp_territory(strftime(bssdt, '%Y'), dbfile),
                       sheetname=sheetname,
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)

  
}



# MAIN PROCESS ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if(interactive()) {
  bssdt = as.Date('2017-11-30')
  genlion_final_dt = as.Date('2017-09-30')
  excelFile = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_PERFORMANCE_REPORT_%s.xlsx", strftime(bssdt,'%Y-%m-%d'))
  exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_PERFORMANCE_REPORT_2017-10-31.xlsx"
  # exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\template_MONTHLY_AGENCY_PERFORMANCE_REPORT_2017-10-04.xlsx"
  
  create_report(bssdt, genlion_final_dt, exceltemplate, excelFile)
}



