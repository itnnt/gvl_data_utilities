# # *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Required libraries ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# library(dplyr)
# library(RSQLite)
# # load xlsx package
# # if (Sys.getenv("JAVA_HOME") != "") {
#   Sys.setenv(JAVA_HOME = "C:\\Program Files\\Java\\jre1.8.0_92")
# # }
# library(xlsx)
# library (lubridate)
# library(zoo)

Sys.setenv(
  # Define common format string
  KPI_PRODUCTION_FORMAT_COMMONDATE = '%d/%m/%Y',
  KPI_PRODUCTION_FORMAT_BUSSINESSDATE = '%Y-%m-%d',
  KPI_PRODUCTION_FORMAT_MONTH_YEAR_y_b = '%b-%y',
  KPI_PRODUCTION_FORMAT_YEAR_y = '%y'
)

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Define common format string ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
FORMAT_COMMONDATE     = Sys.getenv("KPI_PRODUCTION_FORMAT_COMMONDATE")
FORMAT_BUSSINESSDATE  = Sys.getenv("KPI_PRODUCTION_FORMAT_BUSSINESSDATE")
FORMAT_MONTH_YEAR_y_b = Sys.getenv("KPI_PRODUCTION_FORMAT_MONTH_YEAR_y_b")
FORMAT_YEAR_y         = Sys.getenv("KPI_PRODUCTION_FORMAT_YEAR_y")

source('KPI_PRODUCTION/Common functions.R')