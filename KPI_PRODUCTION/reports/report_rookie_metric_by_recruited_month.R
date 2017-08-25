source('KPI_PRODUCTION/Common functions.R')
source('KPI_PRODUCTION/set_environment.r')

private_add_cols_changed_colnames_and_save <- function(df, kpiname) {
  df <- dplyr::mutate(df, kpi=kpiname)
  # replace all by empty except the first 4 chars
  # get the left 4 chars
  df <- dplyr::mutate(df, yy= sub("(^\\d{4}).*", "\\1", df$time_view))
  # jan-17: get the 2 last digits
  # df <- dplyr::mutate(df, yy= sub(".*(\\d+{2}).*$", "\\1", df$time_view))
  # 
  if (!'territory' %in% names(df)) {
    df <- dplyr::mutate(df, territory='')
  }
    
  insert_or_replace_bulk(df, 'report_rookie_metric_by_recruited_month')
  # df
}

rookie_mertric = get_Rookie_Metric_by_Recruited_month_v2(as.Date('2017-07-31'))
private_add_cols_changed_colnames_and_save(
  dplyr::select(rookie_mertric, time_view=recruit_month, value=new_recruited_agents), 
  'recruited_ag_m0')
private_add_cols_changed_colnames_and_save(
  dplyr::select(rookie_mertric, time_view=recruit_month, value=`%actv d1-30`), 
  '%actv d1-30')
private_add_cols_changed_colnames_and_save(
  dplyr::select(rookie_mertric, time_view=recruit_month, value=`%actv d31-60`), 
  '%actv d31-60')
private_add_cols_changed_colnames_and_save(
  dplyr::select(rookie_mertric, time_view=recruit_month, value=`%actv d61-90`), 
  '%actv d61-90')


# 
# for (businessdate in generate_last_day_of_month(2016)) {
#   businessdate = as.Date(businessdate)
#   print(businessdate)
#   # businessdate = as.Date('2017-06-30')
#   get_Rookie_Metric_by_Recruited_month(businessdate) 
#   
#   private_add_cols_changed_colnames_and_save(
#     select(active_d1_d30_ag, BUSSINESSDATE, value=recruited_ag_m0), 
#     'recruited_ag_m0')
#   private_add_cols_changed_colnames_and_save(
#     select(active_d1_d30_ag, BUSSINESSDATE, value=`%actv d1-30`), 
#     '%actv d1-30')
#   
#   private_add_cols_changed_colnames_and_save(
#     select(active_d31_d60_ag, BUSSINESSDATE, value=`%actv d31-60`), 
#     '%actv d31-60')
#   
#   private_add_cols_changed_colnames_and_save(
#     select(active_d61_d90_ag, BUSSINESSDATE, value=`%actv d61-90`), 
#     '%actv d61-90')
# }
