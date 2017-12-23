source('KPI_PRODUCTION/reports/MONTHLY_BANCASSURANCE_PERFORMANCE_TRACKING/banca_performance_function.R')

bssdt <- as.Date('2017-11-30')
bssdt_m1 = get_last_day_of_m1(strftime(bssdt, '%Y'), strftime(bssdt, '%m'))
# banca_performance_function_update(bssdt, sourcefile = "D://workspace_excel//GVL//DOMS//Banca Monthly Performance Tracking//Sources_HA_CAO_CAP_NHAT_DEN_30_11_2017.xlsx")
banca_performance_function_update_v1(bssdt, sourcefile = "D://workspace_excel//GVL//DOMS//Banca Monthly Performance Tracking//Sources_HA_CAO_CAP_NHAT_DEN_30_11_2017.xlsx")

folder = 'D://workspace_excel//GVL//DOMS//Banca Monthly Performance Tracking//'
excelFile = paste(folder, sprintf("MONTHLY_BANCASSURANCE_PERFORMANCE_TRACKING_%s.xlsx", strftime(bssdt,'%Y-%m')), sep = "")
exceltemplate = paste(folder, sprintf("MONTHLY_BANCASSURANCE_PERFORMANCE_TRACKING_%s.xlsx", strftime(bssdt_m1,'%Y-%m')), sep = "")
# exceltemplate = paste(folder, "template_MONTHLY_BANCASSURANCE_PERFORMANCE_TRACKING_201709.xlsx", sep = "")
create_report(bssdt, excelFile, exceltemplate)
