get_group_casesize <- function(group_ape, group_case, casesize_row_index){
  group_ape %>% 
    dplyr::select('time_view', 'segment', 'group_level', 'APE') %>% 
    merge(
      x = .,
      y = group_case %>% dplyr::select('time_view', 'segment', 'group_level', 'CASE'),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(CASESIZE = APE/CASE) %>% 
    dplyr::select(-APE, -CASE) %>% 
    dplyr::mutate(index = paste(segment, casesize_row_index, sep = ''), row_idx = casesize_row_index)
}
get_group_casesize_segment <- function(group_ape, group_case, join_vars){
  group_ape %>% 
    merge(
      x = .,
      y = group_case,
      by.x = join_vars, #c('time_view', 'segment', 'group_level'),
      by.y = join_vars, #c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(CASESIZE = APE/CASE) %>% 
    dplyr::select(-APE, -CASE) %>% 
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.))) # remove columns that have sufix is ".x" or '.y' (if any)
}
get_group_case_per_active_segment <- function(group_case, group_active_agents, join_vars){
  group_case %>% 
    merge(
      x = .,
      y = group_active_agents,
      by.x = join_vars, #c('time_view', 'segment', 'group_level'),
      by.y = join_vars, #c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(CASEPERACTIVE = CASE/ACTIVE) %>% 
    dplyr::select(-ACTIVE, -CASE) %>% 
    dplyr::select(., -grep("*\\.x$|*\\.y$", colnames(.))) # remove columns that have sufix is ".x" or '.y' (if any)
}
get_group_casesize_monthly <- function(group_ape_monthly, group_case_monthly, casesize_row_index){
  group_ape_monthly %>% 
    dplyr::select('time_view', 'segment', 'group_level', 'APE') %>% 
    merge(
      x = .,
      y = group_case_monthly %>% dplyr::select('time_view', 'segment', 'group_level', 'CASE'),
      by.x = c('time_view', 'segment', 'group_level'),
      by.y = c('time_view', 'segment', 'group_level'),
      all = TRUE
    ) %>% 
    dplyr::mutate(CASESIZE = ifelse(is.na(CASE) | is.na(APE) | CASE==0, 0, APE/CASE)) %>% 
    dplyr::select(-APE, -CASE) %>% 
    dplyr::mutate(index = paste(segment, casesize_row_index, sep = ''), row_idx = casesize_row_index)
}
get_group_casesize_ytd <- function(group_ape_ytd, group_case_ytd, casesize_row_index) {
  group_ape_ytd %>% 
  dplyr::select(time_view, segment, group_level, APE_YTD) %>% 
  merge(
    x = .,
    y = group_case_ytd %>% dplyr::select(time_view, segment, group_level, CASE_YTD),
    by.x = c('time_view', 'segment', 'group_level'),
    by.y = c('time_view', 'segment', 'group_level'),
    all = TRUE
  ) %>% 
  dplyr::mutate(CASESIZE_YTD = APE_YTD/CASE_YTD) %>% 
  dplyr::select(-APE_YTD, -CASE_YTD) %>% 
  dplyr::mutate(index = paste(segment, casesize_row_index, sep = ''), row_idx = casesize_row_index)
}
get_group_caseperactive_ytd <- function(group_active_agents_ytd, group_case_ytd, caseperactive_row_index){
  group_active_agents_ytd %>% 
  dplyr::select(time_view, segment, group_level, ACTIVE_YTD) %>% 
  merge(
    x = .,
    y = group_case_ytd %>% dplyr::select(time_view, segment, group_level, CASE_YTD),
    by.x = c('time_view', 'segment', 'group_level'),
    by.y = c('time_view', 'segment', 'group_level'),
    all = TRUE
  ) %>% 
  dplyr::mutate(CASEPERACTIVE_YTD = CASE_YTD/ACTIVE_YTD) %>% 
  dplyr::select(-ACTIVE_YTD, -CASE_YTD) %>% 
  dplyr::mutate(index = paste(segment, caseperactive_row_index, sep = ''), row_idx = caseperactive_row_index)
}
get_casesize_growth <- function(casesize, casesize1) {
  growth = casesize %>%
    dplyr::select('time_view', 'segment', 'group_level', 'CASESIZE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    merge(
      x = .,
      y = casesize1 %>%
        dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
        dplyr::select('time_view_month', 'segment', 'group_level', 'CASESIZE'),
      by.x = c('time_view_month', 'segment', 'group_level'),
      by.y = c('time_view_month', 'segment', 'group_level'),
      all.x = T
    ) 
  
  growth %>% 
    dplyr::mutate('Casesize Growth' = 
                    ifelse(!is.na(CASESIZE.y) & CASESIZE.y != 0, CASESIZE.x/CASESIZE.y-1, 
                           ifelse(!is.na(CASESIZE.x) & CASESIZE.x != 0, 1, 0))) %>% 
    dplyr::select(segment, time_view, 'Casesize Growth') %>% 
    tidyr::spread(time_view, 'Casesize Growth') %>% 
    dplyr::mutate(index = paste(segment, 'Casesize Growth', sep='-'))
}
get_casesize_growth_ytd <- function(casesize, casesize1) {
  # casesize=group_casesize_ytd
  # casesize1=group_casesize_ytd_y1
  growth = casesize %>%
    dplyr::filter(!is.na(segment)) %>% 
    dplyr::select('time_view', 'segment', 'group_level', CASESIZE='CASESIZE_YTD') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6), time_view=substr(time_view, 1,4)) %>% 
    merge(
      x = .,
      y = casesize1 %>%
        dplyr::filter(!is.na(segment)) %>% 
        dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
        dplyr::select('time_view_month', 'segment', 'group_level', CASESIZE='CASESIZE_YTD'),
      by.x = c('time_view_month', 'segment', 'group_level'),
      by.y = c('time_view_month', 'segment', 'group_level'),
      all.x = T
    ) 
  
  growth %>% 
    dplyr::mutate('Casesize Growth' = 
                    ifelse(!is.na(CASESIZE.y) & CASESIZE.y != 0, CASESIZE.x/CASESIZE.y-1, 
                           ifelse(!is.na(CASESIZE.x) & CASESIZE.x != 0, 1, 0))) %>% 
    dplyr::select(segment, time_view, 'Casesize Growth') %>% 
    tidyr::spread(time_view, 'Casesize Growth') %>% 
    dplyr::mutate(index = paste(segment, 'Casesize Growth', sep='-'))
}
get_case_per_active_growth <- function(cpa, cpa1) {
  cpa %>%
    dplyr::select('time_view', 'segment', 'group_level', 'CASEPERACTIVE') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
    merge(
      x = .,
      y = cpa1 %>%
        dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
        dplyr::select('time_view_month', 'segment', 'group_level', 'CASEPERACTIVE'),
      by.x = c('time_view_month', 'segment', 'group_level'),
      by.y = c('time_view_month', 'segment', 'group_level'),
      all.x = T
    ) %>% 
    dplyr::mutate('Case/Active Growth' = 
                    ifelse(!is.na(CASEPERACTIVE.y) & CASEPERACTIVE.y != 0, CASEPERACTIVE.x/CASEPERACTIVE.y-1, 
                           ifelse(!is.na(CASEPERACTIVE.x) & CASEPERACTIVE.x != 0, 1, 0))) %>% 
    dplyr::select(segment, time_view, 'Case/Active Growth') %>% 
    dplyr::filter(!is.na(segment)) %>% 
    tidyr::spread(time_view, 'Case/Active Growth') %>% 
    dplyr::mutate(index = paste(segment, 'Case/Active Growth', sep='-'))
}
get_case_per_active_growth_ytd <- function(cpa, cpa1) {
  cpa %>%
    dplyr::filter(!is.na(segment)) %>% 
    dplyr::select('time_view', 'segment', 'group_level', CASEPERACTIVE='CASEPERACTIVE_YTD') %>%
    dplyr::mutate(time_view_month = substr(time_view, 5,6), time_view=substr(time_view, 1,4)) %>% 
    merge(
      x = .,
      y = cpa1 %>%
        dplyr::filter(!is.na(segment)) %>% 
        dplyr::mutate(time_view_month = substr(time_view, 5,6)) %>% 
        dplyr::select('time_view_month', 'segment', 'group_level', CASEPERACTIVE='CASEPERACTIVE_YTD'),
      by.x = c('time_view_month', 'segment', 'group_level'),
      by.y = c('time_view_month', 'segment', 'group_level'),
      all.x = T
    ) %>% 
    dplyr::mutate('Case/Active Growth' = 
                    ifelse(!is.na(CASEPERACTIVE.y) & CASEPERACTIVE.y != 0, CASEPERACTIVE.x/CASEPERACTIVE.y-1, 
                           ifelse(!is.na(CASEPERACTIVE.x) & CASEPERACTIVE.x != 0, 1, 0))) %>% 
    dplyr::select(segment, time_view, 'Case/Active Growth') %>% 
    tidyr::spread(time_view, 'Case/Active Growth') %>% 
    dplyr::mutate(index = paste(segment, 'Case/Active Growth', sep='-'))
}
