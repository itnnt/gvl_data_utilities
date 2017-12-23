plan_north_rawdata <-read_excel("KPI_PRODUCTION/reports/PLAN_IMPORT/north.xlsx", sheet = 1)

plan_north_kpi = tidyr::gather(plan_north_rawdata, plan_for, value, -kpi_name) %>% 
  dplyr::mutate(time_view = strftime(convertToDate(plan_for), "%Y%m"), plan_for = convertToDate(plan_for))

plan_north_ape = plan_north_kpi %>% 
  dplyr::filter(kpi_name == 'APE') %>% 
  dplyr::mutate(segment = 'NORTH') %>% 
  dplyr::select(segment, time_view, value) %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::mutate(index=paste(segment, 'APE', 'Target', sep = '-'))
