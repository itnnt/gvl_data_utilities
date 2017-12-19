
# Thưởng quý nhóm hoạt động hiệu quả (dành cho UM/SM/BM/SB) ---------------
# 2.5) Thưởng quý nhóm hoạt động hiệu quả (dành cho UM/SM/BM/SB)							
# 
# Thưởng quý nhóm hoạt động hiệu quả = Tổng FYC nhóm trực tiếp trong quý x Tỷ lệ thưởng						
# 
# 
# Tổng FYP nhóm trực tiếp trong quý		Số lượt TVBH của nhóm trực tiếp có hoạt động trong quý				
#                      (triệu đồng)	  Từ 6 đến 9	Từ 10 đến 17	Từ 18 đến 23	Từ 24 trở lên	
#                    Từ 600 trở lên		       20% 	         23%	         26%	         30%	
#                 Từ 360 - dưới 600		       15%	         18%     	     21%	         25%	
#                 Từ 240 - dưới 360		       10%	         13%     	     16%	         20%	
#                 Từ 120 - dưới 240		       5%	           5%	           5%	           5%	
#   
#   - Tỷ lệ duy trì HĐ nhóm:		 
#                               + Năm 1: 85%			
#                               + Năm 2: 75%			
# 
# Các trường hợp UM mới được thăng chức trong quý, doanh số nhóm trực tiếp sẽ được tính kể từ đầu quý bao gồm cả thời gian làm US			
# 

SEG_FYP = c(
  '0 < FYP & FYP < 90',
   '90 <= FYP & FYP < 100',
  '100 <= FYP & FYP < 110',
  '110 <= FYP & FYP < 120',
  '120 <= FYP & FYP < 240',
  '240 <= FYP & FYP < 360',
  '360 <= FYP & FYP < 600',
  '600 <= FYP & FYP < 700',
  '700 <= FYP & FYP < 800',
  '800 <= FYP'
)

SEG_ACTSP = c(
  '1 <= ACTIVE & ACTIVE <= 3',
  'ACTIVE == 4',
  'ACTIVE == 5',
   '6 <= ACTIVE & ACTIVE <= 9',
  '10 <= ACTIVE & ACTIVE <= 17',
  '18 <= ACTIVE & ACTIVE <= 23',
  '24 <= ACTIVE & ACTIVE <= 26',
  '27 <= ACTIVE'
)

eoq1 = as.Date('2017-03-31')
eoq2 = as.Date('2017-06-30')
eoq3 = as.Date('2017-09-30')
end_of_quarters = c(
  as.Date('2017-03-31'),
  as.Date('2017-06-30'),
  as.Date('2017-09-30')
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
bssdt = as.Date('2017-09-30')
dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db'

# data needed for contest calculation -------------------------------------
kpi = get_kpi(strftime(bssdt, '%Y%'), dbfile) # lay het kpi tu dau nam


# activesp_nhom_truc_tiep -------------------------------------------------
bssds = generate_last_day_of_month(2017)
activesp_nhom_truc_tiep <- do.call(rbind,lapply(bssds, function(d) {
  ACTIVESP_UNITCD(as.Date(d), dbfile) 
})
) 

activesp_nhom_truc_tiep <- activesp_nhom_truc_tiep %>% 
  dplyr::mutate(time_view = as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = '-')) %>% 
  dplyr::group_by(time_view, TERRITORY) %>% 
  dplyr::summarise(value = sum(value)) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(SEG = apply_SEG(value, 'ACTIVE', SEG_ACTSP), IDX = apply_SEG(value, 'ACTIVE', SEG_ACTSP, T)) 

# fyp_nhom_truc_tiep ------------------------------------------------------
fyp_nhom_truc_tiep = kpi %>%
  dplyr::mutate(time_view =  as.Date(as.character(kpi$BUSSINESSDATE), '%Y%m%d')) %>% # convert to date
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-")) %>% 
  dplyr::group_by(time_view, UNITCD) %>%
  dplyr::summarise(FYP = sum(FYP, na.rm = T), FYC = sum(FYC, na.rm = T)) %>%
  dplyr::ungroup(.) %>%
  dplyr::mutate(SEG = apply_SEG(FYP/10^6, 'FYP', SEG_FYP), IDX = apply_SEG(FYP/10^6, 'FYP', SEG_FYP, T)) 

# persistency_UNIT -----------------------------------------------------
# apply-like function that returns a data frame
per_unit_y1 <- do.call(rbind,lapply(end_of_quarters, function(d) {
  persistency_UNIT_Y1(d, dbfile)
})
)%>% 
  dplyr::filter(!is.na(UNIT_CODE) & UNIT_CODE != '') %>% 
  dplyr::mutate(SEG = ifelse(PERSISTENCY_Y1 >= 0.85, 'PERSISTENCY_Y1 >= 85%', 'PERSISTENCY_Y1 < 85%')) %>% 
  dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))

# per_unit_y1 = rbind(
#   persistency_UNIT_Y1(eoq1, dbfile)
#   ,
#   persistency_UNIT_Y1(eoq2, dbfile)
#   ,
#   persistency_UNIT_Y1(eoq3, dbfile)
# ) %>% 
#   dplyr::filter(!is.na(UNIT_CODE) & UNIT_CODE != '') %>% 
#   dplyr::mutate(SEG = ifelse(PERSISTENCY_Y1 >= 0.85, 'PERSISTENCY_Y1 >= 85%', 'PERSISTENCY_Y1 < 85%')) %>% 
#   dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
#   dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))


per_unit_y2 <- do.call(rbind,lapply(end_of_quarters, function(d) {
  persistency_UNIT_Y2(d, dbfile)
})
) %>% 
  dplyr::filter(!is.na(UNIT_CODE) & UNIT_CODE != '') %>% 
  dplyr::mutate(SEG = ifelse(PERSISTENCY_Y2 >= 0.75, 'PERSISTENCY_Y2 >= 75%', 'PERSISTENCY_Y2 < 75%')) %>% 
  dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
  dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))
  
# per_unit_y2 = rbind(
#   persistency_UNIT_Y2(as.Date('2017-03-31'), dbfile)
#   ,
#   persistency_UNIT_Y2(as.Date('2017-06-30'), dbfile)
#   ,
#   persistency_UNIT_Y2(as.Date('2017-09-30'), dbfile)
# ) %>% 
#   dplyr::filter(!is.na(UNIT_CODE) & UNIT_CODE != '') %>% 
#   dplyr::mutate(SEG = ifelse(PERSISTENCY_Y2 >= 0.75, 'PERSISTENCY_Y2 >= 75%', 'PERSISTENCY_Y2 < 75%')) %>% 
#   dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
#   dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))

unit_seg = do.call(rbind,lapply(end_of_quarters, function(d) {
  SEGMENT_UM_SUM_BM_SBM(d, dbfile)
})
)
# unit_seg = rbind(
#   SEGMENT_UM_SUM_BM_SBM(as.Date('2017-03-31'), dbfile)
#   ,
#   SEGMENT_UM_SUM_BM_SBM(as.Date('2017-06-30'), dbfile)
#   ,
#   SEGMENT_UM_SUM_BM_SBM(as.Date('2017-09-30'), dbfile)
# )

total = fyp_nhom_truc_tiep %>% mutate(SEG_FYP = SEG) %>% dplyr::select(-SEG) %>% 
  merge(x = .,
        y = activesp_nhom_truc_tiep %>% mutate(SEG_ACTSP = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'UNITCD'),
        by.y = c('time_view', 'TERRITORY'),
        all = T) %>% 
  merge(x = .,
        y = per_unit_y1 %>% mutate(SEG_Y1 = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'UNITCD'),
        by.y = c('time_view', 'UNIT_CODE'),
        all.x = T) %>% 
  merge(x = .,
        y = per_unit_y2 %>% mutate(SEG_Y2 = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'UNITCD'),
        by.y = c('time_view', 'UNIT_CODE'),
        all.x = T)
total1 = fyp_nhom_truc_tiep %>% mutate(SEG_FYP = SEG) %>% dplyr::select(-SEG) %>% 
  merge(x = .,
        y = activesp_nhom_truc_tiep %>% mutate(SEG_ACTSP = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'UNITCD'),
        by.y = c('time_view', 'TERRITORY'),
        all = T) %>% 
  merge(x = .,
        y = per_unit_y1 %>% mutate(SEG_Y1 = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'UNITCD'),
        by.y = c('time_view', 'UNIT_CODE'),
        all.x = T) %>% 
  merge(x = .,
        y = per_unit_y2 %>% mutate(SEG_Y2 = SEG) %>% dplyr::select(-SEG),
        by.x = c('time_view', 'UNITCD'),
        by.y = c('time_view', 'UNIT_CODE'),
        all.x = T) %>% 
  merge(x = .,
        y = unit_seg,
        by.x = c('time_view', 'UNITCD'),
        by.y = c('time_view', 'UNIT_CODE'),
        all.x = T)


total %>% 
  dplyr::group_by(
    time_view, 
    SEG_FYP=ifelse(is.na(SEG_FYP) | SEG_FYP=='', 'OTHERS', SEG_FYP), 
    IDX.x=ifelse(is.na(IDX.x) | IDX.x=='', 11, IDX.x)
  ) %>% 
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  dplyr::arrange(as.numeric(IDX.x)) %>% View

total %>% 
  dplyr::group_by(time_view, SEG_ACTSP=ifelse(is.na(SEG_ACTSP), 'OTHERS', SEG_ACTSP), IDX.y) %>% 
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  dplyr::arrange(IDX.y) %>% 
  dplyr::select(-IDX.y) %>% View

# View persistency y1 of unit
total %>% 
  dplyr::group_by(time_view, SEG_Y1=ifelse(is.na(SEG_Y1), 'OTHERS', SEG_Y1)) %>%
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  View
# View persistency y2 of unit
total %>% 
  dplyr::group_by(time_view, SEG_Y2=ifelse(is.na(SEG_Y2), 'OTHERS', SEG_Y2)) %>%
  dplyr::summarise(count_unit = n()) %>% 
  dplyr::ungroup(.) %>% 
  tidyr::spread(time_view, count_unit) %>% 
  View

# 
# xem phan bo fyp va active KHONG XET PERSISTENCY: Q1
# 
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
View(criteria)

#
#---------------
#

# 
# xem phan bo fyp va active KHONG XET PERSISTENCY: Q1 - xet rieng UM
# 
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q1' & total1$SEG == 'UM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

#
#---------------
#

# 
# xem phan bo fyp va active KHONG XET PERSISTENCY: Q1 - xet rieng SUM
# 
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q1' & total1$SEG == 'SUM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

#
#---------------
#

# 
# xem phan bo fyp va active KHONG XET PERSISTENCY: Q1 - xet rieng BM
# 
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q1' & total1$SEG == 'BM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

#
#---------------
#

# 
# xem phan bo fyp va active KHONG XET PERSISTENCY: Q1 - xet rieng SBM
# 
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q1' & total1$SEG == 'SBM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

#
#---------------
#

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
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q2 - XET RIENG UM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q2' & total1$SEG == 'UM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q2 - XET RIENG SUM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q2' & total1$SEG == 'SUM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q2 - XET RIENG BM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q2' & total1$SEG == 'BM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q2 - XET RIENG SBM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q2' & total1$SEG == 'SBM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

#
#---------------
#

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
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q3 - XET RIENG UM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q3' & total1$SEG == 'UM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q3 - XET RIENG SUM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q3' & total1$SEG == 'SUM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q3 - XET RIENG BM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q3' & total1$SEG == 'BM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

# xem phan bo fyp va active KHONG XET PERSISTENCY: Q3 - XET RIENG SBM
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total1[total1$time_view == '2017-Q3' & total1$SEG == 'SBM',]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

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
View(criteria)

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
View(criteria)

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
View(criteria)

# xem phan bo fyp va active XET PERSISTENCY Y1 & Y2: Q1
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q1' & 
                (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85) & 
                (is.na(total$PERSISTENCY_Y2) | total$PERSISTENCY_Y2 >= 0.75),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)


# xem phan bo fyp va active XET PERSISTENCY Y1 & Y2: Q2
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q2' & 
                (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85) & 
                (is.na(total$PERSISTENCY_Y2) | total$PERSISTENCY_Y2 >= 0.75),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)

# xem phan bo fyp va active XET PERSISTENCY Y1 & Y2: Q3
criteria = data.frame(matrix(0,ncol = length(SEG_ACTSP), nrow = length(SEG_FYP)), row.names = SEG_FYP)
names(criteria) <- SEG_ACTSP
temdt = total[total$time_view == '2017-Q3' & 
                (is.na(total$PERSISTENCY_Y1) | total$PERSISTENCY_Y1 >= 0.85) & 
                (is.na(total$PERSISTENCY_Y2) | total$PERSISTENCY_Y2 >= 0.75),]
for (i in 1:nrow(temdt)) {
  fyp = temdt[i, 'FYP']
  active = ifelse(is.na(temdt[i, 'value']), 0, temdt[i, 'value'])
  if (!is.na(fyp) & !is.na(active)) {
    apply_criteria(FYP = fyp/10^6, ACTIVE = active, criteria)
  }
} 
View(criteria)


# 
# 
# 
# # activesp_ban_truc_tiep -------------------------------------------------
# activesp_ban_truc_tiep = rbind(
#   ACTIVESP_BRANCHCD(as.Date('2017-03-31'), genlion_final_dt = as.Date('2017-03-31'), dbfile) 
#   ,
#   ACTIVESP_BRANCHCD(as.Date('2017-06-30'), genlion_final_dt = as.Date('2017-03-31'), dbfile) 
#   ,
#   ACTIVESP_BRANCHCD(as.Date('2017-09-30'), genlion_final_dt = as.Date('2017-06-30'), dbfile) 
# ) %>% 
#   dplyr::filter(kpi == '# Active_by_rookie_mdrt:Total') %>% 
#   dplyr::mutate(SEG = seg_active(value), IDX = seg_active_idx(value)) %>% 
#   dplyr::mutate(time_view = paste(time_view, '01', sep = '')) %>% 
#   dplyr::mutate(time_view = as.Date(strptime(time_view, '%Y%m%d'))) %>% 
#   dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = '-'))
# 
# activesp_ban_truc_tiep %>% 
#   dplyr::group_by(time_view, SEG, IDX) %>% 
#   dplyr::summarise(count_unit = n()) %>% 
#   dplyr::ungroup(.) %>% 
#   tidyr::spread(time_view, count_unit) %>% 
#   dplyr::arrange(IDX) %>% 
#   dplyr::select(-IDX) %>% View
# 
# 
# 
# # fyp_ban_truc_tiep -------------------------------------------------------
# fyp_ban_truc_tiep = kpi %>%
#   dplyr::mutate(time_view =  as.Date(as.character(kpi$BUSSINESSDATE), '%Y%m%d')) %>% # convert to date
#   # dplyr::mutate(time_view_quarter = quarters(time_view)) %>% # get quarter of the month
#   dplyr::group_by(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"),
#                   # time_view_quarter,
#                   BRANCHCD) %>%
#   dplyr::summarise(FYP = sum(FYP, na.rm = T), FYC = sum(FYC, na.rm = T)) %>%
#   dplyr::ungroup(.) %>%
#   dplyr::mutate(SEG = seg(FYP), IDX = idx(FYP))
# 
# fyp_ban_truc_tiep %>% 
#   dplyr::group_by(time_view, SEG, IDX) %>% 
#   dplyr::summarise(count_unit = n()) %>% 
#   dplyr::ungroup(.) %>% 
#   tidyr::spread(time_view, count_unit) %>% 
#   dplyr::arrange(IDX) %>% 
#   dplyr::select(-IDX) %>% View
# 
# # persistency_BRANCH -----------------------------------------------------
# per_branch_y1 = rbind(
#   persistency_BRANCH_Y1(as.Date('2017-03-31'), dbfile)
#   ,
#   persistency_BRANCH_Y1(as.Date('2017-06-30'), dbfile)
#   ,
#   persistency_BRANCH_Y1(as.Date('2017-09-30'), dbfile)
# ) %>% 
#   dplyr::filter(!is.na(BRANCHCD) & BRANCHCD != '') %>% 
#   dplyr::mutate(SEG = ifelse(PERSISTENCY_Y1 >= 0.85, 'PERSISTENCY_Y1 >= 85%', 'PERSISTENCY_Y1 < 85%')) %>% 
#   dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
#   dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))
# 
# per_branch_y2 = rbind(
#   persistency_BRANCH_Y2(as.Date('2017-03-31'), dbfile)
#   ,
#   persistency_BRANCH_Y2(as.Date('2017-06-30'), dbfile)
#   ,
#   persistency_BRANCH_Y2(as.Date('2017-09-30'), dbfile)
# ) %>% 
#   dplyr::filter(!is.na(BRANCHCD) & BRANCHCD != '') %>% 
#   dplyr::mutate(SEG = ifelse(PERSISTENCY_Y2 >= 0.75, 'PERSISTENCY_Y2 >= 75%', 'PERSISTENCY_Y2 < 75%')) %>% 
#   dplyr::mutate(time_view =  as.Date(as.character(BSSDT), '%Y-%m-%d')) %>% # convert to date
#   dplyr::mutate(time_view = paste(strftime(time_view, '%Y'), quarters(time_view), sep = "-"))
# 
# total_branch = fyp_ban_truc_tiep %>% mutate(SEG_FYP = SEG) %>% dplyr::select(-SEG) %>% 
#   merge(x = .,
#         y = activesp_ban_truc_tiep %>% mutate(SEG_ACTSP = SEG) %>% dplyr::select(-SEG),
#         by.x = c('time_view', 'BRANCHCD'),
#         by.y = c('time_view', 'territory'),
#         all = T) %>% 
#   merge(x = .,
#         y = per_branch_y1 %>% mutate(SEG_Y1 = SEG) %>% dplyr::select(-SEG),
#         by.x = c('time_view', 'BRANCHCD'),
#         by.y = c('time_view', 'BRANCHCD'),
#         all.x = T) %>% 
#   merge(x = .,
#         y = per_branch_y2 %>% mutate(SEG_Y2 = SEG) %>% dplyr::select(-SEG),
#         by.x = c('time_view', 'BRANCHCD'),
#         by.y = c('time_view', 'BRANCHCD'),
#         all.x = T)
# 
# seg_final(total_branch) %>% 
#   dplyr::group_by(time_view, BONUS_RATE) %>% 
#   dplyr::summarise(n = n()) %>% 
#   tidyr::spread(time_view, n) %>% 
#   dplyr::arrange(desc(as.numeric(gsub('%', '', BONUS_RATE)))) %>% 
#   View
# # View persistency y1 of unit
# total_branch %>% 
#   dplyr::group_by(time_view, SEG_Y1) %>%
#   dplyr::summarise(count_unit = n()) %>% 
#   dplyr::ungroup(.) %>% 
#   tidyr::spread(time_view, count_unit) %>% 
#   View
# # View persistency y2 of unit
# total_branch %>% 
#   dplyr::group_by(time_view, SEG_Y2) %>%
#   dplyr::summarise(count_unit = n()) %>% 
#   dplyr::ungroup(.) %>% 
#   tidyr::spread(time_view, count_unit) %>% 
#   View
# 
