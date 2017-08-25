source('KPI_PRODUCTION/Common functions.R')

excelFile = sprintf("KPI_PRODUCTION/output/GVL_Agency_reports_%s.xlsx", strftime(Sys.time(),'%y%m%d'))
# sheet North -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

t2=report_kpi_segmentation(criteria = "where territory='NORTH' and yy>=15")
# t2[,'value'] <- as.numeric(t2[,'value'],)
t3=tidyr::spread(t2, time_view, value)
# set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
rownames(t3)<-gsub("^\\s+|\\s+$", "", t3[,'kpi'])

replace_cellvalue2.1(t3,
                     sheetname="North",
                     rowNameColIndex = 1,
                     headerRowIndex = 1,
                     template="KPI_PRODUCTION/output/Agency Performance by Segmentation/GVL_Agency_report_template.xlsx",
                     result_file=excelFile)


# sheet South -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


t2=report_kpi_segmentation(criteria = "where territory='SOUTH' and yy>=15")
# t2[,'value'] <- as.numeric(t2[,'value'],)
t3=tidyr::spread(t2, time_view, value)
# set rownames equal to column1
rownames(t3)<-t3[,'kpi']

replace_cellvalue2.1(t3,
                     sheetname="South",
                     rowNameColIndex = 1,
                     headerRowIndex = 1,
                     template=excelFile,
                     result_file=excelFile)



