library(RDCOMClient)
xlApp <- COMCreate("Excel.Application")
xlApp$Quit()               # close running Excel
wb    <- xlApp[["Workbooks"]]$Open("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/reports/reports - Copy.xlsx")
sheet <- wb$Worksheets("reports")

# Make Excel workbook visible to user
xlApp[["Visible"]] <- T

# change the value of a single cell
cell  <- sheet$Cells(11,12)
cell[["Value"]] <- 'may ha 3.2322323'
cell[["Interior"]][["ColorIndex"]] <- "33"
cell[['Interior']][['Color']] = 33322

sheet$Cells(11,12)[["Select"]]
# cell[["Select"]][["Fill"]] <- 3
cell[["ForeColor"]][["ObjectThemeColor"]] = 5

RDCOMClient_set_cellvalue(wb, "reports", 11, 13, 'tao day')

# change the value of a range
range <- sheet$Range("A1:F1")
range[["Value"]] <- paste("Col",1:6,sep="-")

# Force to Overwrite An Excel File through RDCOMClient Package in R, 
# Ghi đè file sẵn có không cần thông báo
xlApp[["DisplayAlerts"]] <- FALSE

# wb$Save()                  # save the workbook
wb$SaveAS("D:\\workspace_r\\gvl_data_utilities\\KPI_PRODUCTION\\output\\new.file122wwwwww.xlsx")  # save as a new workbook
xlApp[["Workbooks"]]$Open("D:\\workspace_r\\gvl_data_utilities\\KPI_PRODUCTION\\output\\new.file122wwwwww.xlsx")

# xlApp$Quit()               # close Excel
