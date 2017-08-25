
# *** ---------------------------------------------------------------------
# Model_Algorithms.R ------------------------------------------------------
source("KPI_PRODUCTION/set_environment.r")
source("KPI_PRODUCTION/Model_Algorithms.R")
source("KPI_PRODUCTION/Model_Algorithms_North.R")
source("KPI_PRODUCTION/Model_Algorithms_South.R")

# *** ---------------------------------------------------------------------
# functions ---------------------------------------------------------------
sheet_region_kpi <- function(df, workbook, sheetName, row_name_prefixes, offset=1) {
  sheet0 <- xlsx::createSheet(workbook, sheetName=sheetName) # create different sheets
  # add data frame to the sheets
  
  for (i in 1:length(row_name_prefixes)) {
    pre = row_name_prefixes[i]
    selected_row_indexes = grep(sprintf("^%s", pre), rownames(df))
    if (length(selected_row_indexes)) {
      temdt = as.data.frame(df[selected_row_indexes,])
      # remove prefix from colnames 
      rownames(temdt) <- gsub(pre, "", rownames(temdt))
      startRow = ifelse(i==1,(selected_row_indexes[1] + offset),(selected_row_indexes[1]+i+(i*offset-1)))
      print(startRow)
      xlsx::addDataFrame(
        temdt,
        sheet0,
        row.names=T, startRow = startRow,
        colnamesStyle = TABLE_COLNAMES_STYLE,
        rownamesStyle = TABLE_ROWNAMES_STYLE
      )
      # Set cell stype for all the rows excluded row is headers
      rows  <- getRows(sheet0, rowIndex=(startRow+1):(startRow+1+nrow(temdt)))   
      cells <- xlsx::getCells(rows, colIndex = 2:(ncol(temdt)+1)) # get all the cells excluded the 1st col
      lapply(names(cells), function(ii) xlsx::setCellStyle(cells[[ii]], NUMERIC_STYLE))
      # format percentage for YoY, MoM columns
      cellsYoY <- xlsx::getCells(rows, colIndex = 1+grep("YoY", names(temdt)))
      lapply(names(cellsYoY), function (x) xlsx::setCellStyle(cellsYoY[[x]], PERCENTAGE_STYLE))
      cellsMoM <- xlsx::getCells(rows, colIndex = 1+grep("MoM", names(temdt)))
      lapply(names(cellsMoM), function (x) xlsx::setCellStyle(cellsMoM[[x]], PERCENTAGE_STYLE))
      
      # format cel A1 Overall KPI performance
      cell11 <- getCells(getRows(sheet0,rowIndex=startRow:(startRow+1)),colIndex = 1:2)[[1]]
      xlsx::setCellValue(cell11, gsub("[_]*[:]"," ", pre))
      xlsx::setCellStyle(cell11, TABLE_COLNAMES_STYLE_BLUE)
      
      # autosize column widths for all the columns in the sheet
      xlsx::autoSizeColumn(sheet0, colIndex=1:(1+ncol(df)))
      
      # replace separated columns' values by sep1
      header_row  <- getRows(sheet0, rowIndex=startRow)   # get the header row
      cellsSeps <- xlsx::getCells(header_row, colIndex = 1+grep("^sep", names(df)))
      lapply(names(cellsSeps), function (x) xlsx::setCellValue(cellsSeps[[x]], ''))
    }
  }
  
  # Create a freeze pane, fix first row and column
  xlsx::createFreezePane(sheet0, colSplit = 2, rowSplit = 2+offset)
  sheet0
}


# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Save output to excel file -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
wb <- xlsx::createWorkbook(type="xlsx")         # create blank workbook

# *** ---------------------------------------------------------------------
# Styles for the data table row/column names ------------------------------
NUMERIC_STYLE <- xlsx::CellStyle(wb, dataFormat =  xlsx::DataFormat('#,##0.0')) 
# + Fill(foregroundColor="lightblue", backgroundColor="lightblue", pattern="SOLID_FOREGROUND")   
# + Font(wb, isBold=TRUE)

PERCENTAGE_STYLE <- xlsx::CellStyle(wb, dataFormat =  xlsx::DataFormat('#,##0.0%')) #+ Font(wb, isBold=TRUE)

TABLE_ROWNAMES_STYLE <- xlsx::CellStyle(wb) + Font(wb, isBold=TRUE) +
  Alignment(wrapText=TRUE, horizontal="ALIGN_RIGHT")

TABLE_COLNAMES_STYLE <- xlsx::CellStyle(wb) + Font(wb, isBold=TRUE) +
  Alignment(wrapText=TRUE, horizontal="ALIGN_RIGHT") +
  Border(color="black", position=c("TOP", "BOTTOM"), 
         pen=c("BORDER_THIN", "BORDER_THIN")) 

TABLE_COLNAMES_STYLE_BLUE <- xlsx::CellStyle(wb) + Font(wb, isBold=TRUE, color = 'blue') +
  # Alignment(wrapText=TRUE, horizontal="ALIGN_CENTER") +
  Alignment(wrapText=TRUE, horizontal="ALIGN_RIGHT") +
  Border(color="blue", position=c("TOP", "BOTTOM"), 
         pen=c("BORDER_THIN", "BORDER_THIN")) 

# _ sheet0: North ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# add sheets to the workbook
sheet0 <- sheet_region_kpi(North_results, wb, "North", c("Overall KPI performance:","Ending MP:", "Recruitment:", "AL recruitment KPIs:", "APE:", "# Active:"))
sheet1 <- sheet_region_kpi(South_results, wb, "South", c("Overall KPI performance:","Ending MP:", "Recruitment:", "AL recruitment KPIs:", "APE:", "# Active:"))


# _ save the work book ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# write the file with multiple sheets
xlsx::saveWorkbook(wb, sprintf("KPI_PRODUCTION/output/GVL_Agency_reports_%s.xlsx", strftime(Sys.time(),'%y%m%d_%H%M%S')))  

