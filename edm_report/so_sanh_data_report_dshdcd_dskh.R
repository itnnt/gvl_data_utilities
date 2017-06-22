library(readr)
danh_sach_hd_ca_nhan_AG004496 <- read_csv("D:/workspace_r/gvl_data_utilities/edm_report/danh_sach_hd_ca_nhan_AG004496.csv")
danh_sach_khach_hang_AG004496 <- read_csv("D:/workspace_r/gvl_data_utilities/edm_report/danh_sach_khach_hang_AG004496.csv")

a1<-unique(danh_sach_hd_ca_nhan_AG004496$policynum)
a2<-unique(danh_sach_khach_hang_AG004496$POLICYNO)

paste(setdiff(a1, a2), collapse = ",")

