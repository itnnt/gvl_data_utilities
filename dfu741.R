library(readxl)
library(dplyr)
kpidetail <-
  read_excel("S:/IT/Share to others/tung/DFUWF-741-1/DFU_CheckDay_20170228_2.xlsx",
             +sheet = "KPI_DETAIL")
View(kpidetail)
kpisum <-
  read_excel("S:/IT/Share to others/tung/DFUWF-741-1/DFU_CheckDay_20170228_2.xlsx",
             +sheet = "KPI_SUM")
View(kpisum)
# edm calculated but tool did not ####
kpisum_edm_cal<-kpisum %>%
  filter(!is.na(EDM), is.na(TOOL) | TOOL == 0)
View(kpisum_edm_cal)
