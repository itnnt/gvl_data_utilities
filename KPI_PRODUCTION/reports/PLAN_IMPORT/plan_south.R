plan_south_rawdata <-read_excel("KPI_PRODUCTION/reports/PLAN_IMPORT/south.xlsx", sheet = 1)

plan_south_kpi = tidyr::gather(plan_south_rawdata, plan_for, value, -kpi_name) %>% 
  dplyr::mutate(time_view = strftime(convertToDate(plan_for), "%Y%m"), plan_for = convertToDate(plan_for))

plan_south_ape = plan_south_kpi %>% 
  dplyr::filter(kpi_name == 'APE') %>% 
  dplyr::mutate(segment = 'SOUTH') %>% 
  dplyr::select(segment, time_view, value) %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::mutate(index=paste(segment, 'APE', 'Target', sep = '-'))
