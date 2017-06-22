library(rJava)
library(xlsx)
# load xlsx package
if (Sys.getenv("JAVA_HOME") != "") {
  Sys.setenv(JAVA_HOME = "")
}

xlsx_write_to_file <- function(data, filename) {
  # save data to excel file
  write.xlsx(data, paste(filename, ".xlsx", sep = ''), row.names = FALSE)
}

xlsx_write_to_file_multi_sheets <-
  function(data, filename, sheet_name) {
    # save data to excel file
    write.xlsx(
      data,
      paste(filename, ".xlsx", sep = ''),
      sheetName = sheet_name,
      append = TRUE,
      row.names = FALSE,
      showNA = FALSE
    )
  }

xlsx_write2_to_file <- function(data, filename) {
  # save data to excel file
  fr = 0
  for (x in seq(5000, nrow(data), 5000)) {
    print(sprintf("from %d to %d", fr, x))
    fr = x + 1
    write.xlsx2(
      data[fr:x, ],
      paste(filename, ".xlsx", sep = ''),
      row.names = FALSE,
      append = TRUE
    )
  }
  
}

xlsx_createworkbook <- function(data) {
  wb <- xlsx::createWorkbook()         # create blank workbook
  sheet1 <-
    createSheet(wb, sheetName = "mysheet1") # create different sheets
  sheet2 <- createSheet(wb, sheetName = "mysheet2")
  
  offset = 5000
  for (x in seq(offset, nrow(data), offset)) {
    print(sprintf("from %d to %d", x - offset + 1, x))
    addDataFrame(data[x - offset + 1:x, ], sheet1, startRow = x - offset +
                   1)  # add data to the sheets
  }
  saveWorkbook(wb, "mysheets2.xlsx")  # w
}

save_to_sql_file <- function(sqls, filename, append = FALSE) {
  write.table(
    sqls,
    filename,
    quote = FALSE,
    sep = ",",
    row.names = FALSE,
    col.names = FALSE,
    append = append
  )
}