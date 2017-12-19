terminated_agent_fyp_in_segment <- function(bssdt) {
  # bssdt = as.Date('2017-02-28')
  terminated_agents = termination_movement(bssdt, dbfile)
  agent_insegment = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, bssdt, dbfile, index_from=7)
  terminated_agent_fyp = fyp_individual_10per_topup_incl(bssdt, dbfile) %>% 
    dplyr::group_by(AGCODE) %>% 
    dplyr::summarise(FYP = sum(FYP)) %>% 
    dplyr::ungroup() %>% 
    merge(
      x = .,
      y = terminated_agents,
      by.x = 'AGCODE',
      by.y = 'AGENT_CODE',
      all.y = T
    ) %>% 
    dplyr::mutate('FYP NULL' = ifelse(is.na(FYP), 1, 0)) %>% 
    dplyr::mutate('FYP < 0' = ifelse(!is.na(FYP) & FYP < 0, 1, 0)) %>% 
    dplyr::mutate('FYP = 0' = ifelse(!is.na(FYP) & FYP == 0, 1, 0)) %>% 
    dplyr::mutate('FYP <= 5' = ifelse(!is.na(FYP) & 0 < FYP/10^6 & FYP/10^6 <= 5, 1, 0)) %>% 
    dplyr::mutate('FYP <= 15' = ifelse(!is.na(FYP) & 5 < FYP/10^6 & FYP/10^6 <= 15, 1, 0)) %>% 
    dplyr::mutate('FYP <= 30' = ifelse(!is.na(FYP) & 15 < FYP/10^6 & FYP/10^6 <= 30, 1, 0)) %>% 
    dplyr::mutate('FYP > 30' = ifelse(!is.na(FYP) & FYP/10^6 > 30, 1, 0))
  
  re = rbind(
   terminated_agent_fyp %>% 
    dplyr::group_by(TERRITORY, time_view) %>% 
    dplyr::summarise(
                     `FYP NULL`=sum(`FYP NULL`, na.rm = T), 
                     `FYP < 0`=sum(`FYP < 0`, na.rm = T), 
                     `FYP = 0`=sum(`FYP = 0`, na.rm = T), 
                     `FYP <= 5`=sum(`FYP <= 5`, na.rm = T), 
                     `FYP <= 15`=sum(`FYP <= 15`, na.rm = T), 
                     `FYP <= 30`=sum(`FYP <= 30`, na.rm = T),
                     `FYP > 30`=sum(`FYP > 30`, na.rm = T)) %>% 
     dplyr::ungroup() %>% 
    tidyr::gather(segment, value, - TERRITORY, -time_view)
  ,
   terminated_agent_fyp %>% 
    dplyr::group_by(TERRITORY = 'COUNTRY', time_view) %>% 
    dplyr::summarise(`FYP NULL`=sum(`FYP NULL`, na.rm = T), 
                     `FYP < 0`=sum(`FYP < 0`, na.rm = T), 
                     `FYP = 0`=sum(`FYP = 0`, na.rm = T), 
                     `FYP <= 5`=sum(`FYP <= 5`, na.rm = T), 
                     `FYP <= 15`=sum(`FYP <= 15`, na.rm = T), 
                     `FYP <= 30`=sum(`FYP <= 30`, na.rm = T),
                     `FYP > 30`=sum(`FYP > 30`, na.rm = T)) %>% 
    dplyr::ungroup() %>% 
    tidyr::gather(segment, value, - TERRITORY, -time_view)
  )
  
  try((re[,'idx'] = 0), silent = T)
  try((re[re['segment']== 'FYP NULL','idx'] = 1), silent = T)
  try((re[re['segment']== 'FYP < 0','idx'] = 2), silent = T)
  try((re[re['segment']== 'FYP = 0','idx'] = 3), silent = T)
  try((re[re['segment']== 'FYP <= 5','idx'] = 4), silent = T)
  try((re[re['segment']== 'FYP <= 15','idx'] = 5), silent = T)
  try((re[re['segment']== 'FYP <= 30','idx'] = 6), silent = T)
  try((re[re['segment']== 'FYP > 30','idx'] = 7), silent = T)
  re
}

terminated_agent_count_in_segment <- function(bssdt) {
  terminated_agents = termination_movement(bssdt, dbfile)
  agent_insegment = SEGMENT_AGENTS_IN_SA_GENLION_MDRT_ROOKIE_2.3_4.6_7.12_13UPMONTHS_lv.TERRITORY(bssdt, bssdt, dbfile, index_from=7)
  rbind(
  # territory
  terminated_agents %>% 
    merge(x=.,y=agent_insegment, by.x='AGENT_CODE', by.y='AGENT_CODE') %>% 
    dplyr::group_by(time_view,SEG, territory,level, idx) %>% 
    dplyr::summarise(value = n()) %>% 
    dplyr::ungroup() 
  ,
  # country
  terminated_agents %>% 
    merge(x=.,y=agent_insegment, by.x='AGENT_CODE', by.y='AGENT_CODE') %>% 
    dplyr::group_by(time_view,SEG, territory = 'COUNTRY',level = 'COUNTRY', idx) %>% 
    dplyr::summarise(value = n()) %>% 
    dplyr::ungroup()
  ) %>% 
    dplyr::arrange(idx)
}


terminated_agent_ytd = do.call(rbind, lapply(generate_last_day_of_month(2017), function(d) {
  terminated_agent_count_in_segment(d)
})
)
terminated_agent_fyp_ytd = do.call(rbind, lapply(generate_last_day_of_month(2017), function(d) {
  terminated_agent_fyp_in_segment(d)
})
)

# write terminated agent manpower to excel
write_from = 2
excelFile = 'KPI_PRODUCTION/reports/TERMINATED_AGENTS/terminated_agents.xlsx'
terminated_agent_ytd_country = rbind(
  terminated_agent_ytd %>% 
  dplyr::filter(territory == 'COUNTRY') %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::arrange(idx)
,
# total row
terminated_agent_ytd %>% 
  dplyr::filter(territory == 'COUNTRY') %>% 
  dplyr::group_by(time_view, SEG = 'Total', territory, level, idx=0) %>% 
  dplyr::summarise(value=sum(value, na.rm = T)) %>% 
  dplyr::ungroup() %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::arrange(idx)
)

write_from = fill_excel_column1.4_no_fmt(
  df = terminated_agent_ytd_country,
  sheetname = "manpower",
  rowNameColIndex = 1,
  headerRowIndex = 1,
  start_writing_from_row = write_from,
  template = excelFile,
  result_file = excelFile
) 
terminated_agent_ytd_north = rbind(
  terminated_agent_ytd %>% 
  dplyr::filter(territory == 'NORTH') %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::arrange(idx)
  ,
  # total row
  terminated_agent_ytd %>% 
    dplyr::filter(territory == 'NORTH') %>% 
    dplyr::group_by(time_view, SEG = 'Total', territory, level, idx=0) %>% 
    dplyr::summarise(value=sum(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(time_view, value) %>% 
    dplyr::arrange(idx)
)
write_from = fill_excel_column1.4_no_fmt(
  df = terminated_agent_ytd_north,
  sheetname = "manpower",
  rowNameColIndex = 1,
  headerRowIndex = 1,
  start_writing_from_row = write_from + 2,
  template = excelFile,
  result_file = excelFile
) 
terminated_agent_ytd_south = rbind(
  terminated_agent_ytd %>% 
  dplyr::filter(territory == 'SOUTH') %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::arrange(idx)
  ,
  # total row
  terminated_agent_ytd %>% 
    dplyr::filter(territory == 'SOUTH') %>% 
    dplyr::group_by(time_view, SEG = 'Total', territory, level, idx=0) %>% 
    dplyr::summarise(value=sum(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(time_view, value) %>% 
    dplyr::arrange(idx)
)

write_from = fill_excel_column1.4_no_fmt(
  df = terminated_agent_ytd_south,
  sheetname = "manpower",
  rowNameColIndex = 1,
  headerRowIndex = 1,
  start_writing_from_row = write_from + 2,
  template = excelFile,
  result_file = excelFile
) 

write_from = 2
terminated_agent_fyp_ytd_country = rbind(
  terminated_agent_fyp_ytd %>% 
  dplyr::filter(TERRITORY == 'COUNTRY') %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::mutate(SEG = segment) %>% 
  dplyr::arrange(-idx)
,
terminated_agent_fyp_ytd %>% 
  dplyr::filter(TERRITORY == 'COUNTRY') %>% 
  dplyr::group_by(TERRITORY, time_view, segment='Total', idx = 0) %>% 
  dplyr::summarise(value = sum(value, na.rm = T)) %>% 
  dplyr::ungroup() %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::mutate(SEG = segment)
)

write_from = fill_excel_column1.4_no_fmt(
  df = terminated_agent_fyp_ytd_country,
  sheetname = "fyp",
  rowNameColIndex = 1,
  headerRowIndex = 1,
  start_writing_from_row = write_from,
  template = excelFile,
  result_file = excelFile
)
terminated_agent_fyp_ytd_north = rbind(
  terminated_agent_fyp_ytd %>% 
  dplyr::filter(TERRITORY == 'NORTH') %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::mutate(SEG = segment) %>% 
  dplyr::arrange(-idx)
  ,
  terminated_agent_fyp_ytd %>% 
    dplyr::filter(TERRITORY == 'NORTH') %>% 
    dplyr::group_by(TERRITORY, time_view, segment='Total', idx = 0) %>% 
    dplyr::summarise(value = sum(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(time_view, value) %>% 
    dplyr::mutate(SEG = segment)
)
write_from = fill_excel_column1.4_no_fmt(
  df = terminated_agent_fyp_ytd_north,
  sheetname = "fyp",
  rowNameColIndex = 1,
  headerRowIndex = 1,
  start_writing_from_row = write_from + 2,
  template = excelFile,
  result_file = excelFile
)
terminated_agent_fyp_ytd_south = rbind(
  terminated_agent_fyp_ytd %>% 
  dplyr::filter(TERRITORY == 'SOUTH') %>% 
  tidyr::spread(time_view, value) %>% 
  dplyr::mutate(SEG = segment) %>% 
  dplyr::arrange(-idx)
  ,
  terminated_agent_fyp_ytd %>% 
    dplyr::filter(TERRITORY == 'SOUTH') %>% 
    dplyr::group_by(TERRITORY, time_view, segment='Total', idx = 0) %>% 
    dplyr::summarise(value = sum(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(time_view, value) %>% 
    dplyr::mutate(SEG = segment)
)
write_from = fill_excel_column1.4_no_fmt(
  df = terminated_agent_fyp_ytd_south,
  sheetname = "fyp",
  rowNameColIndex = 1,
  headerRowIndex = 1,
  start_writing_from_row = write_from + 2,
  template = excelFile,
  result_file = excelFile
)
