# 3.2) Thưởng quý Ban hoạt động hiệu quả					
# 
# Thưởng quý Ban hoạt động hiệu quả = Tổng FYC Ban trực tiếp trong quý x Tỷ lệ thưởng				
# 
# Tổng FYP của Ban trực tiếp trong quý 
#                         (triệu đồng)    Tỷ lệ thưởng
#                                         (% FYC)		
#                      Từ 2300 trở lên	  10.0%		
#                  Từ 1500 - dưới 2300	  8.0%		
#                   Từ 960 - dưới 1500	  6.0%		
#                    Từ 440 - dưới 960 	  4.0%		
# 
# - Điều kiện:				
#   +Số lượt TVBH của Ban trực tiếp có hoạt động tối thiểu: 27			
# 
#   +Tỷ lệ hoạt động trung bình quý: >=25%			
#   +Tỷ lệ duy trì HĐ Ban cuối quý	
#                                   Năm 1: 85%	
#                                   Năm 2: 70%	


SEG_FYP = c(
  'FYP < 350',
  #  '90 <= FYP & FYP < 100',
  # '100 <= FYP & FYP < 110',
  # '110 <= FYP & FYP < 120',
  '350 <= FYP & FYP < 400',
  '400 <= FYP & FYP < 440',
  '440 <= FYP & FYP < 960',
  '960 <= FYP & FYP < 1500',
  '1500 <= FYP & FYP < 2300',
  '2300 <= FYP'
)

SEG_ACTSP = c(
  'ACTIVE < 4',
   '4 <= ACTIVE & ACTIVE <= 5',
   '6 <= ACTIVE & ACTIVE <= 9',
  '10 <= ACTIVE & ACTIVE <= 17',
  '18 <= ACTIVE & ACTIVE <= 23',
  '24 <= ACTIVE & ACTIVE <= 26',
  '27 <= ACTIVE'
)

# functions ---------------------------------------------------------------
apply_criteria <- function(FYP, ACTIVE, criteria) {
  criteria_for_row = row.names(criteria)
  criteria_for_col = names(criteria)
  for (r in criteria_for_row) {
    for (c in criteria_for_col) {
      eval_r = eval(parse(text = r))
      eval_c = eval(parse(text = c))
      if (eval_r & eval_c) {
        criteria[r, c] <<- criteria[r, c] + 1
      }
    }
  }
}

# config param ------------------------------------------------------------
source('KPI_PRODUCTION/Common functions.R')
bssdt = as.Date('2017-08-31')
dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db'

# data needed for contest calculation -------------------------------------
kpi = get_kpi(strftime(bssdt, '%Y%'), dbfile) # lay het kpi tu dau nam


# activesp_ban_truc_tiep -------------------------------------------------
bssds = generate_last_day_of_month(2017)
activesp_ban_truc_tiep <- do.call(rbind,lapply(bssds, function(d) {
  ACTIVESP_BRANCHCD(as.Date(d), dbfile) 
})
) 

activesp_ban_truc_tiep <- activesp_ban_truc_tiep %>% 
  dplyr::mutate(time_view = as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = '-')) %>% 
  dplyr::group_by(time_view, TERRITORY) %>% 
  dplyr::summarise(value = sum(value, na.rm=T)) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(SEG = apply_SEG(value, 'ACTIVE', SEG_ACTSP), IDX = apply_SEG(value, 'ACTIVE', SEG_ACTSP, T)) 

# fyp_ban_truc_tiep ------------------------------------------------------
fyp_ban_truc_tiep = kpi %>%
  dplyr::mutate(time_view =  as.Date(as.character(kpi$BUSSINESSDATE), '%Y%m%d')) %>% # convert to date
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-")) %>% 
  dplyr::group_by(time_view, BRANCHCD) %>%
  dplyr::summarise(FYP = sum(FYP, na.rm = T), FYC = sum(FYC, na.rm = T)) %>%
  dplyr::ungroup(.) %>%
  dplyr::mutate(SEG = apply_SEG(FYP/10^6, 'FYP', SEG_FYP), IDX = apply_SEG(FYP/10^6, 'FYP', SEG_FYP, T)) 

# persistency_UNIT -----------------------------------------------------
per_unit_y1 = rbind(
  persistency_BRANCH_Y1(as.Date('2017-03-31'), dbfile)
  ,
  persistency_BRANCH_Y1(as.Date('2017-06-30'), dbfile)
  ,
  persistency_BRANCH_Y1(as.Date('2017-08-31'), dbfile)
) %>% 
  dplyr::filter(!is.na(BRANCHCD) & BRANCHCD != '') %>% 
  dplyr::mutate(SEG = ifelse(PERSISTENCY_Y1 >= 0.85, 'PERSISTENCY_Y1 >= 85%', 'PERSISTENCY_Y1 < 85%')) %>% 
  dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))

per_unit_y2 = rbind(
  persistency_BRANCH_Y2(as.Date('2017-03-31'), dbfile)
  ,
  persistency_BRANCH_Y2(as.Date('2017-06-30'), dbfile)
  ,
  persistency_BRANCH_Y2(as.Date('2017-08-31'), dbfile)
) %>% 
  dplyr::filter(!is.na(BRANCHCD) & BRANCHCD != '') %>% 
  dplyr::mutate(SEG = ifelse(PERSISTENCY_Y2 >= 0.7, 'PERSISTENCY_Y2 >= 70%', 'PERSISTENCY_Y2 < 70%')) %>% 
  dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))

total = fyp_ban_truc_tiep %>% mutate(SEG_FYP = SEG) %>% dplyr::select(-SEG) %>% 
  merge(x = .,
        y = activesp_ban_truc_tiep %>% mutate(SEG_ACTSP = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'BRANCHCD'),
        by.y = c('time_view', 'TERRITORY'),
        all = T) %>% 
  merge(x = .,
        y = per_unit_y1 %>% mutate(SEG_Y1 = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'BRANCHCD'),
        by.y = c('time_view', 'BRANCHCD'), 
        all.x = T
       ) %>% 
  merge(x = .,
        y = per_unit_y2 %>% mutate(SEG_Y2 = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'BRANCHCD'),
        by.y = c('time_view', 'BRANCHCD'),
        all.x = T
        )

total %>% 
  dplyr::group_by(time_view, SEG_FYP, IDX.x) %>% 
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  dplyr::arrange(IDX.x) %>% View

total %>% 
  dplyr::group_by(time_view, SEG_ACTSP, IDX.y) %>% 
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  dplyr::arrange(IDX.y) %>% 
  dplyr::select(-IDX.y) %>% View

# View persistency y1 of unit
total %>% 
  dplyr::group_by(time_view, SEG_Y1) %>%
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  View
# View persistency y2 of unit
total %>% 
  dplyr::group_by(time_view, SEG_Y2) %>%
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  View

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q1
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q1',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q2
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q2',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q3
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q3',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active XET PERSISTENCY Y1: Q1
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q1' & (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active XET PERSISTENCY Y1: Q2
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q2' & (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active XET PERSISTENCY Y1: Q3
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q3' & (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active XET PERSISTENCY Y1 & Y2: Q1
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q1' & 
                (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85) & 
                (is.na(total$PERSISTENCY_Y2) | total$PERSISTENCY_Y2 >= 0.7),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active XET PERSISTENCY Y1 & Y2: Q2
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q2' & 
                (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85) & 
                (is.na(total$PERSISTENCY_Y2) | total$PERSISTENCY_Y2 >= 0.7),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 

# xem phan bo fyp va active XET PERSISTENCY Y1 & Y2: Q3
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q3' & 
                (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85) & 
                (is.na(total$PERSISTENCY_Y2) | total$PERSISTENCY_Y2 >= 0.7),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 







