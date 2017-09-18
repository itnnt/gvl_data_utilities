source('KPI_PRODUCTION/Common functions.R')







create_report <- function (bssdt){
  excelFile = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_PERFORMANCE_REPORT_%s.xlsx", strftime(Sys.time(),'%Y-%m-%d'))
  exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\template_MONTHLY_AGENCY_PERFORMANCE_REPORT.xlsx"
  # 2.0 Manpower ------------------------------------------------------------
  mp = kpi_segmentation(criteria = sprintf("where kpi like '%s' and level='COUNTRY' and time_view  like '%s' ", "# Manpower%", strftime(bssdt, '%Y%')))
  
  
  
  # 4.0 Segmentation --------------------------------------------------------
  kpi_segmentation(criteria = sprintf("where level='COUNTRY' AND kpi like '%s' and time_view='%s'", 'Activity Ratio%', strftime(bssdt, '%Y%m'))) %>% 
    print
  
  # 3.0 Rookies -------------------------------------------------------------
  sheetname = '3.0 Rookies'
  
  recruit = kpi_segmentation(criteria = "where kpi='# Manpower_by_rookie_mdrt:Rookie in month' and level='TERRITORY'") %>% 
    dplyr::mutate(idx=1) %>% 
    dplyr::mutate(kpi='recruit')
  recruit_country = recruit %>% 
    dplyr::group_by(time_view, kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  recruit_ytd = recruit %>% 
    dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
    dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  recruit_country_ytd = recruit_country %>% 
    dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
    dplyr::group_by(time_view=substr(time_view, 1, 4), kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  rookie_metric = get_Rookie_Performance(bssdt)%>% 
    dplyr::mutate(idx=2) %>% 
    dplyr::ungroup(.)
  rookie_metric_country = rookie_metric %>% 
    dplyr::group_by(time_view, kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  rookie_metric_ytd = rookie_metric %>% 
    dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
    dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)
  rookie_metric_country_ytd = rookie_metric %>% 
    dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
    dplyr::group_by(time_view=substr(time_view, 1, 4), kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.)  
  rookie_metric_percentage = rookie_metric %>% 
    base::merge(x=., y=recruit, by.x=c('time_view', 'territory'), by.y=c('time_view', 'territory')) %>% 
    dplyr::mutate(value=paste(formatC(100 * value.x/value.y, format = "f", digits = 1),'%', sep = '')) %>% 
    dplyr::mutate(idx=3) %>% 
    dplyr::mutate(kpi=paste('%', kpi.x, sep='')) %>% 
    dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
    dplyr::select(., -grep('*\\.y$', colnames(.))) 
  rookie_metric_percentage_country = rookie_metric_country %>% 
    base::merge(x=., y=recruit_country, by.x=c('time_view'), by.y=c('time_view')) %>% 
    dplyr::mutate(value=paste(formatC(100 * value.x/value.y, format = "f", digits = 1),'%', sep = '')) %>% 
    dplyr::mutate(idx=3) %>% 
    dplyr::mutate(kpi=paste('%', kpi.x, sep='')) %>% 
    dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
    dplyr::select(., -grep('*\\.y$', colnames(.))) 
  rookie_metric_ytd_percentage = rookie_metric_ytd %>% 
    base::merge(x=., y=recruit_ytd, by.x=c('time_view', 'territory'), by.y=c('time_view', 'territory')) %>% 
    dplyr::mutate(value=paste(formatC(100 * value.x/value.y, format = "f", digits = 1),'%', sep = '')) %>% 
    dplyr::mutate(idx=3) %>% 
    dplyr::mutate(kpi=paste('%', kpi.x, sep='')) %>% 
    dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
    dplyr::select(., -grep('*\\.y$', colnames(.))) 
  rookie_metric_percentage_country_ytd = rookie_metric_country_ytd %>% 
    base::merge(x=., y=recruit_country_ytd, by.x=c('time_view'), by.y=c('time_view')) %>% 
    dplyr::mutate(value=paste(formatC(100 * value.x/value.y, format = "f", digits = 1),'%', sep = '')) %>% 
    dplyr::mutate(idx=3) %>% 
    dplyr::mutate(kpi=paste('%', kpi.x, sep='')) %>% 
    dplyr::select(., -grep('*\\.x$',colnames(.))) %>% 
    dplyr::select(., -grep('*\\.y$', colnames(.))) 
  dt = rbind(
    recruit
    ,
    rookie_metric 
    ,
    rookie_metric_percentage
    ,
    recruit_ytd
    ,
    rookie_metric_ytd
    ,
    rookie_metric_ytd_percentage
  ) %>% 
    tidyr::spread(., time_view, value) 
  
  dt = dt[with(dt, order(territory, idx)),] 
  dt = dt %>% 
    dplyr::mutate(level='TERRITORY') %>% 
    dplyr::mutate(province='') %>% 
    dplyr::mutate(region='') %>% 
    dplyr::mutate(zone='') %>% 
    dplyr::mutate(name=territory) 
  
  fill_excel_column1.3(
    df=dt,
    sheetname="Rookie Metric",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 8,
    template=excelFile,
    result_file=excelFile
  )
  
  dt_country = rbind(
    recruit_country,
    recruit_country_ytd,
    rookie_metric_country,
    rookie_metric_country_ytd,
    rookie_metric_percentage_country,
    rookie_metric_percentage_country_ytd
  ) %>% 
    tidyr::spread(., time_view, value) 
  dt_country = dt_country[with(dt_country, order(idx)),] 
  dt_country = dt_country %>% 
    dplyr::mutate(territory='') %>% 
    dplyr::mutate(level='COUNTRY') %>% 
    dplyr::mutate(province='') %>% 
    dplyr::mutate(region='') %>% 
    dplyr::mutate(zone='') %>% 
    dplyr::mutate(name='') 
  fill_excel_column1.2(
    df=dt_country,
    sheetname="Rookie Metric",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 4+nrow(dt),
    template=excelFile,
    result_file=excelFile,
    "#00CCFF"
  )
  
  # 5.0 Product MIx ---------------------------------------------------------
  sheetname = '5.0 Product MIx'
  t1 = product_mix(criteria = "WHERE product_code not in ('RIDER', 'SME')", orderby = strftime(bssdt, '%Y')  ,addTotalRow = T)
  fill_excel_column1.3(
    df=t1 %>% dplyr::filter(product_code != 'Basic Total'),
    sheetname=sheetname,
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 8,
    template=excelFile,
    result_file=excelFile
  )
  
  t2=rbind(
    t1 %>% dplyr::filter(product_code == 'Basic Total'),
    product_mix(criteria = "WHERE product_code in ('RIDER', 'SME')", orderby = strftime(bssdt, '%Y'))
  )
  t2$product_name=''
  fill_excel_column1.3(
    df=t2,
    sheetname=sheetname,
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 8+nrow(t1),
    template=excelFile,
    result_file=excelFile,
    '#00CCFF'
  )
  fill_excel_column1.3(
    df=(
      tidyr::gather(t2, time_view, value, -product_code, -product_name, -fmt) %>% 
        dplyr::group_by(time_view, fmt) %>% 
        dplyr::summarise(value=sum(value, na.rm = T)) %>% 
        dplyr::mutate(product_code='Grand Total') %>% 
        dplyr::mutate(product_name='') %>% 
        tidyr::spread(., time_view, value)
    ),
    sheetname=sheetname,
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 8+nrow(t1)+nrow(t2),
    template=excelFile,
    result_file=excelFile,
    '#993366'
  )
  
  t3 = kpi_segmentation(criteria = "where kpi='APE_by_rookie_mdrt:Total' and level='TERRITORY'") %>% 
    dplyr::group_by(time_view) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    dplyr::mutate(product_code='APE (bil)', fmt='#,##0') %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(product_name='') %>% 
    fill_excel_column1.3(
      df=.,
      sheetname=sheetname,
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 8+nrow(t1)+nrow(t2)+1,
      template=excelFile,
      result_file=excelFile,
      '#CC99FF'
    )
  
  # 6.0 GA Performance ------------------------------------------------------
  sheetname = '6.0 GA Performance'
  GA_per = GA(bssdt)
  GA_per = GA_per[order(GA_per[sprintf('%s_APE', strftime(bssdt, '%Y%m'))], decreasing = T),]
  fill_excel_column1(
    df=GA_per,
    sheetname=sheetname,
    rowNameColIndex = 1,
    headerRowIndex = 5,
    start_writing_from_row = 9,
    template=excelFile,
    result_file=excelFile
  )
  
  
}



# MAIN PROCESS ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if(interactive()) {
  bssdt = as.Date('2017-08-31')
  create_report(bssdt)
}



