source('KPI_PRODUCTION/Common functions.R')

excelFile = sprintf("KPI_PRODUCTION/output/201707_GVL_Agency reports_v1.xlsm", strftime(Sys.time(),'%y%m%d'))
# sheet North -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

t2=report_kpi_segmentation(criteria = "where territory='NORTH' and yy>=17")
# t2[,'value'] <- as.numeric(t2[,'value'],)
t3=tidyr::spread(t2, time_view, value)
# set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
rownames(t3)<-gsub("^\\s+|\\s+$", "", t3[,'kpi'])

replace_cellvalue2.1(t3,
                     sheetname="HN",
                     rowNameColIndex = 1,
                     headerRowIndex = 1,
                     template="KPI_PRODUCTION/output/Agency Performance by Segmentation/201707_GVL_Agency reports_v1_template2.xlsm",
                     result_file=excelFile)


# sheet South -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


t2=report_kpi_segmentation(criteria = "where territory='SOUTH' and yy>=17")
# t2[,'value'] <- as.numeric(t2[,'value'],)
t3=tidyr::spread(t2, time_view, value)
# set rownames equal to column1
rownames(t3)<-t3[,'kpi']

replace_cellvalue2.1(t3,
                     sheetname="HCM",
                     rowNameColIndex = 1,
                     headerRowIndex = 1,
                     template=excelFile,
                     result_file=excelFile)



