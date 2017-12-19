get_agency_man_power <- function(bssdt){
  mp = manpower_in_segment(bssdt)
  re = rbind (
    mp %>% 
    tidyr::spread(time_view, MANPOWER) %>% 
    dplyr::arrange(idx)
  ,
    mp %>% 
      dplyr::filter(SEG != "SA") %>% 
      dplyr::group_by(time_view, SEG="Total (excl. SA)", idx=NA, segment, index=NA) %>% 
      dplyr::summarise(MANPOWER = sum(MANPOWER, na.rm = T)) %>% 
      dplyr::ungroup() %>% 
      tidyr::spread(time_view, MANPOWER) 
  ,
    mp %>% 
      dplyr::group_by(time_view, SEG="Total", idx=NA, segment, index=NA) %>% 
      dplyr::summarise(MANPOWER = sum(MANPOWER, na.rm = T)) %>% 
      dplyr::ungroup() %>% 
      tidyr::spread(time_view, MANPOWER) 
  ) 
  re
}


