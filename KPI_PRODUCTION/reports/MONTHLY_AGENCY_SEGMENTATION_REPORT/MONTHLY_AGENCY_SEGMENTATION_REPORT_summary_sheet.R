save_summary_sheet <- function(bssdt, exceltemplate, excelFile){
  sheetname = 'Summary'
  # update Overall Performance table 
  bssdt=as.Date("2017-10-31")
  # ape of the month
  ape = ape_individual_10per_topup_incl(bssdt, dbfile)  
  group_ape = group_data_sum(ape, 'APE') %>%
    dplyr::mutate(APE = APE/10^6, index = paste(segment, ape_row_index, sep = ''), row_idx = ape_row_index) %>% 
    dplyr::filter(segment == 'COUNTRY')
  
  # manpower of the month
  manpower = get_manpower_individual(bssdt, dbfile) 
  group_manpower = group_data_n(manpower, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index) %>% 
    dplyr::filter(segment == 'COUNTRY')
  
  manpower_sa_excl = get_manpower_individual_sa_excl(bssdt, dbfile) 
  group_manpower_excl = group_data_n(manpower_sa_excl, 'MANPOWER')%>%
    dplyr::mutate(index = paste(segment, manpower_row_index, sep = ''), row_idx = manpower_row_index) %>% 
    dplyr::filter(segment == 'COUNTRY')
  
  newrecruit_agents = new_recruit_individual(bssdt, dbfile)  
  group_newrecruit_agents = group_data_n(newrecruit_agents, 'NEWRECRUIT') %>% 
    dplyr::mutate(index = paste(segment, new_recruite_row_index, sep = ''), row_idx = new_recruite_row_index)  %>% 
    dplyr::filter(segment == 'COUNTRY')
  
  # check lai
  group_active_ratio = get_group_active_ratio(group_active_agents, group_manpower, group_manpower_m1, active_ratio_row_index)
  group_active_ratio = group_active_ratio %>% dplyr::filter(segment == 'COUNTRY') 
  
  # check lai
  group_active_ratio_sa_excl = get_group_active_ratio(group_active_agents_sa_excl, 
                                                      group_manpower_sa_excl, 
                                                      group_manpower_sa_excl_m1, 
                                                      active_ratio_sa_excl_row_index)
  group_active_ratio_sa_excl = group_active_ratio_sa_excl %>% dplyr::filter(segment == 'COUNTRY') # check lai
  
  
  
}

