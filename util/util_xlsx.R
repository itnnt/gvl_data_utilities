library(rJava)
library(xlsx)
# load xlsx package
if(Sys.getenv("JAVA_HOME")!=""){
  Sys.setenv(JAVA_HOME="")
}

xlsx_write_to_file <- function(data, filename) {
  # save data to excel file
  write.xlsx(data, paste(filename, ".xlsx", sep = ''))
}