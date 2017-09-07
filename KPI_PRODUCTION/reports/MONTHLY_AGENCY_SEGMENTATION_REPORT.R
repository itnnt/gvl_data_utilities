source('KPI_PRODUCTION/Common functions.R')

# dt=get_segmentation_by_genlion_sa_rookie(as.Date('2017-08-31'))


# Manpower(bssdt =as.Date('2017-07-31'), genlion_final_dt = as.Date('2017-03-31'), dbfile = "d:\\workspace_r\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db")
# Manpower(bssdt =as.Date('2017-07-31'), genlion_final_dt = as.Date('2017-03-31'))
#
# ---
#
update_kpi_segmentation <- function() {
  bssdt = as.Date('2017-07-31')
  genlion_final_dt = as.Date('2017-03-31')
  #----
  get_Rookie_Performance(bssdt) %>%  dplyr::mutate(level='REGION') %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Case_per_Active(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Case(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  CaseSize(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Activity_Ratio(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Active(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Recruitment(bssdt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  APE(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  RYP(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Ending_MP(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Manpower(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  
}

#
# import data phan nay se bo di khi thay the bang tu tinh -----------------
#
exceldatafile="d:\\workspace_r\\gvl_data_utilities\\KPI_PRODUCTION\\output\\Agency Performance by Segmentation\\Agency Performance by Segmentation_2015_201707.xlsx"
df <-read_excel(exceldatafile, 
                sheet = "Agency North")
df <- df[!(is.na(df[,1])),] # remove rows that have no value in the columm 1
df <- df[,!(is.na(colnames(df)))] # remove columns that have no column names
df <- dplyr::select(df, -grep("\\X__*", colnames(df))) # remove columns that have prefix is "X__" (if any)
df <- df[df$kpi!=6,]
df <- dplyr::select(df, c('kpi','201701','201702','201703','201704', '201705', '201706', '201707'))
t1=tidyr::gather(df, time_view, value, -kpi)
t1[,'territory']='NORTH'
# t1[,'yy']=sub(".*(\\d+{2}).*$", "\\1", t1$time_view) # left 2 characters
t1[,'yy']=sub("(^\\d{4}).*", "\\1", t1$time_view) # get the left 4 chars
t1 = dplyr::filter(t1, !is.na(value)) %>% 
     dplyr::mutate(level='REGION')
insert_or_replace_bulk(t1, 'kpi_segmentation') # save to database



df <-read_excel(exceldatafile, 
                sheet = "Agency South")
df <- df[!(is.na(df[,1])),] # remove rows that have no value in the columm 1
df <- df[,!(is.na(colnames(df)))] # remove columns that have no column names
df <- dplyr::select(df, -grep("\\X__*", colnames(df))) # remove columns that have prefix is "X__" (if any)
df <- df[df$kpi!=6,]
df <- dplyr::select(df, c('kpi','201701','201702','201703','201704', '201705', '201706', '201707'))
t1=tidyr::gather(df, time_view, value, -kpi)
t1[,'territory']='SOUTH'
# t1[,'yy']=sub(".*(\\d+{2}).*$", "\\1", t1$time_view) # left 2 characters
t1[,'yy']=sub("(^\\d{4}).*", "\\1", t1$time_view) # get the left 4 chars
t1 = dplyr::filter(t1, !is.na(value))%>% 
     dplyr::mutate(level='REGION')
insert_or_replace_bulk(t1, 'kpi_segmentation') # save to database


df <-read_excel(exceldatafile, 
                sheet = "GEN Lion NORTH")
df <- df[!(is.na(df[,1])),]  # remove rows that have no value in the columm 1
df <- df[(df[,1]) !='',]
df <- df[,!(is.na(colnames(df)))] # remove columns that have no column names
df <- df[,""!=(colnames(df))]
df <- dplyr::select(df, -grep("\\X__*", colnames(df))) # remove columns that have prefix is "X__" (if any)
df <- df[df$kpi!=6,]
df <- dplyr::select(df, c('kpi','201704', '201705', '201706', '201707'))
t1=tidyr::gather(df, time_view, value, -kpi)
t1[,'territory']='NORTH'
t1 <- t1[!is.na(t1$value),] # remove na values from t1
t1[,'yy']=sub("(^\\d{4}).*", "\\1", t1$time_view) # get the left 4 chars
t1 = t1 %>% 
  dplyr::mutate(level='REGION')
insert_or_replace_bulk(t1, 'kpi_segmentation')


df <-read_excel(exceldatafile, 
                sheet = "GEN Lion SOUTH")
df <- df[!(is.na(df[,1])),]  # remove rows that have no value in the columm 1
df <- df[(df[,1]) !='',]
df <- df[,!(is.na(colnames(df)))] # remove columns that have no column names
df <- df[,""!=(colnames(df))]
df <- dplyr::select(df, -grep("\\X__*", colnames(df))) # remove columns that have prefix is "X__" (if any)
df <- df[df$kpi!=6,]
df <- dplyr::select(df, c('kpi','201704', '201705', '201706', '201707'))
t1=tidyr::gather(df, time_view, value, -kpi)
t1[,'territory']='SOUTH'
t1 <- t1[!is.na(t1$value),] # remove na values from t1
t1[,'yy']=sub("(^\\d{4}).*", "\\1", t1$time_view) # get the left 4 chars
t1 = t1%>% 
  dplyr::mutate(level='REGION')
insert_or_replace_bulk(t1, 'kpi_segmentation')

df <-read_excel(exceldatafile, 
                sheet = "GEN Lion GVL")
df <- df[!(is.na(df[,1])),]  # remove rows that have no value in the columm 1
df <- df[(df[,1]) !='',]
df <- df[,!(is.na(colnames(df)))] # remove columns that have no column names
df <- df[,""!=(colnames(df))]
df <- dplyr::select(df, -grep("\\X__*", colnames(df))) # remove columns that have prefix is "X__" (if any)
df <- df[df$kpi!=6,]
# df <- dplyr::select(df, c('kpi','201704', '201705', '201706', '201707'))
t1=tidyr::gather(df, time_view, value, -kpi)
t1[,'territory']='COUNTRY'
t1 <- t1[!is.na(t1$value),] # remove na values from t1
t1[,'yy']=sub("(^\\d{4}).*", "\\1", t1$time_view) # get the left 4 chars
t1 = t1%>% 
  dplyr::mutate(level='COUNTRY')
insert_or_replace_bulk(t1, 'kpi_segmentation')


# product mix -------------------------------------------------------------
exceldatafile="d:\\workspace_r\\gvl_data_utilities\\KPI_PRODUCTION\\output\\Agency Performance by Segmentation\\Product mix_201707 - Copy.xlsx"
df <-read_excel(exceldatafile, 
                sheet = "Sheet1")
df <- df[!(is.na(df[,1])),] # remove rows that have no value in the columm 1
df <- df[,!(is.na(colnames(df)))] # remove columns that have no column names
df <- dplyr::select(df, -grep("\\X__*", colnames(df))) # remove columns that have prefix is "X__" (if any)
# df <- df[df$kpi!=6,]
# df <- dplyr::select(df, c('kpi','201704', '201705', '201706', '201707'))
t1=tidyr::gather(df, time_view, value, -product_code, -product_name)
t1 = dplyr::filter(t1, !is.na(value))
insert_or_replace_bulk(t1, 'product_mix') # save to database


#
# end segment -------------------------------------------------------------
#


#
# update data -------------------------------------------------------------
#
for (bsdt in generate_last_day_of_month(2015)) {
  rs <- active_recruit_leader(as.Date(bsdt)) %>% dplyr::mutate(level='REGION') %>% # active recruit leader: us, um, bm
  insert_or_replace_bulk(., 'kpi_segmentation')
}



# get_Manpower(as.Date('2017-07-31')) %>% 
#   dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, FORMAT_BUSSINESSDATE))) %>% 
#   dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, FORMAT_BUSSINESSDATE)))
#   
# end segment -------------------------------------------------------------


#
# create report -----------------------------------------------------------
#
create_report <- function (bssdt){
  excelFile = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_SEGMENTATION_REPORT_%s.xlsx", strftime(Sys.time(),'%Y-%m-%d'))
  exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\template_MONTHLY_AGENCY_SEGMENTATION_REPORT.xlsx"
  
  # 
  # sheet North -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  #
  
  t2=kpi_segmentation(criteria = "where territory='NORTH' and level='REGION'")
  # t2[,'value'] <- as.numeric(t2[,'value'],)
  t3=tidyr::spread(t2, time_view, value)
  # set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
  rownames(t3)<-gsub("^\\s+|\\s+$", "", t3[,'kpi'])
  
  replace_cellvalue2.1(t3,
                       sheetname="North",
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=exceltemplate,
                       result_file=excelFile)
  
  # RDCOMClient_replace_cellvalues(df=t3,
  #                                excelfile=exceltemplate,
  #                                newexcelfile=excelFile,
  #                                sheetname='North', 
  #                                visible=F, 
  #                                rowNameColIndex=1, 
  #                                headerRowIndex=1
  # )
  
  #
  # sheet South -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  #
  
  t2=kpi_segmentation(criteria = "where territory='SOUTH' and level='REGION'")
  # t2[,'value'] <- as.numeric(t2[,'value'],)
  t3=tidyr::spread(t2, time_view, value)
  # set rownames equal to column1
  rownames(t3)<-t3[,'kpi']
  
  replace_cellvalue2.1(t3,
                       sheetname="South",
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
 
  # RDCOMClient_replace_cellvalues(df=t3,
  #                                excelfile=excelFile,
  #                                newexcelfile=excelFile,
  #                                sheetname='South', 
  #                                visible=F, 
  #                                rowNameColIndex=1, 
  #                                headerRowIndex=1
  #                                )
  
  # 5.0 AG retention --------------------------------------------------------
  df=report_agent_retention(criteria = "where value!=0 and value is not null")
  df=tidyr::spread(df, time_view, value)
  # set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
  rownames(df)<-gsub("^\\s+|\\s+$", "", df[,'kpi'])
  
  replace_cellvalue2.1(df,
                       sheetname="5.0 AG retention",
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  
  # RDCOMClient_replace_cellvalues(
  #   df=df,
  #   excelfile=exceltemplate,
  #   newexcelfile = excelresult,
  #   sheetname="5.0 AG retention",
  #   visible=T,
  #   rowNameColIndex=1,
  #   headerRowIndex=1
  # )
  
  # update product_mix -------------------------------------------------------------
  t2=product_mix(criteria = "")
  # t2[,'value'] <- as.numeric(t2[,'value'],)
  t3=tidyr::spread(t2, time_view, value)
  t3=t3[order(t3$Total, decreasing = T),]
  # set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
  rownames(t3)<-gsub("^\\s+|\\s+$", "", t3[,'product_code'])
  
  replace_cellvalue2.1(t3,
                       sheetname="Product Mix",
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  
  # GA performance ----------------------------------------------------------------
  GA_per = GA(bssdt)
  GA_per = GA_per[order(GA_per$`201707_APE`, decreasing = T),]
  fill_excel_column1(
    df=GA_per,
    sheetname="GA",
    rowNameColIndex = 1,
    headerRowIndex = 5,
    start_writing_from_row = 9,
    template=excelFile,
    result_file=excelFile
  )
  
  # BD performance ----------------------------------------------------------
  BD_per = BD(bssdt)
  BD_per = BD_per[with(BD_per, order(REGION_NAME,ZONE_NAME,TEAM_NAME)),]
  fill_excel_column1.2(
    df=BD_per,
    sheetname="BD",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 8,
    template=excelFile,
    result_file=excelFile
  )
  # RDCOMClient_fill_excel_column(
  #   df=BD_per,
  #   excelfile=excelFile,
  #   newexcelfile = excelFile,
  #   sheetname="BD",
  #   visible=F,
  #   rowNameColIndex=1,
  #   headerRowIndex=1,
  #   start_writing_from_row = 9
  # )
  
# Rookie Metric -----------------------------------------------------------
 
  # apply-like function that returns a data frame
  # recruit <- do.call(rbind,lapply(generate_last_day_of_month(2017), function(d) {
  #   get_Manpower(d, included_ter_ag = T)
  # })
  # )
  
    
  # recruit = get_Manpower(bssdt, included_ter_ag = T) %>% 
  #   dplyr::mutate(JOINING_DATE=as.Date(strptime(JOINING_DATE, '%Y-%m-%d'))) %>% 
  #   dplyr::mutate(BUSSINESSDATE=as.Date(strptime(BUSSINESSDATE, '%Y-%m-%d'))) %>% 
  #   # calculate different months between joining date and business date
  #   dplyr::mutate(MDIFF = as.integer(round((as.yearmon(BUSSINESSDATE) - as.yearmon(JOINING_DATE)) * 12))) %>% 
  #   dplyr::filter(MDIFF == 0) # new recuit %>% 
  #   dplyr::group_by(territory=TERRITORY, time_view=strftime(BUSSINESSDATE,'%Y%m')) %>% 
  #   dplyr::summarise(value=n()) %>% 
  #   # tidyr::spread(., BUSSINESSDATE, value) %>% 
  #   dplyr::mutate(kpi='recruit')  %>% 
  #   dplyr::mutate(idx=1)
  
  recruit = kpi_segmentation(criteria = "where kpi='# Manpower_by_rookie_mdrt:Rookie in month' and level='REGION'") %>% 
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
  
  fill_excel_column1.2(
    df=dt,
    sheetname="Rookie Metric",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 4,
    template=excelFile,
    result_file=excelFile,
    "#CCFFFF"
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
  
  # Ending MP_Structure -----------------------------------------------------
  total = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:Total' and level='REGION' ") %>% 
    dplyr::mutate(idx=1) %>% 
    dplyr::mutate(kpi='Ending Manpower_Total')
  total_excl_sa = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:Total (excl. SA)' and level='REGION' ") %>% 
    dplyr::mutate(idx=2) %>% 
    dplyr::mutate(kpi='Ending Manpower_ExSA')
  sa = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SA' and level='REGION' ") %>% 
    dplyr::mutate(idx=3) %>% 
    dplyr::mutate(kpi='# SA')
  ag = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:AG' and level='REGION' ") %>% 
    dplyr::mutate(idx=4) %>% 
    dplyr::mutate(kpi='# AG')
  us = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:US' and level='REGION' ") %>% 
    dplyr::mutate(idx=5) %>% 
    dplyr::mutate(kpi='# US')
  um = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:UM' and level='REGION' ") %>% 
    dplyr::mutate(idx=6) %>% 
    dplyr::mutate(kpi='# US')
  sum = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SUM' and level='REGION' ") %>% 
    dplyr::mutate(idx=7) %>% 
    dplyr::mutate(kpi='# SUM')
  bm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:BM' and level='REGION' ") %>% 
    dplyr::mutate(idx=8) %>% 
    dplyr::mutate(kpi='# BM')
  sbm = kpi_segmentation(criteria = "where kpi='# Manpower_by_designation:SBM' and level='REGION' ") %>% 
    dplyr::mutate(idx=9) %>% 
    dplyr::mutate(kpi='# SBM')
  
  mp_group_by_territory = rbind(
    total, total %>% 
            dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
            dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
            dplyr::summarise(value=sum(value)) %>% 
            dplyr::ungroup(.)
    
    ,
    total_excl_sa, total_excl_sa %>% 
                    dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
                    dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
                    dplyr::summarise(value=sum(value)) %>% 
                    dplyr::ungroup(.)
    
    ,
    sa, sa %>% 
        dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
        dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
        dplyr::summarise(value=sum(value)) %>% 
        dplyr::ungroup(.)
    ,
    ag, ag %>% 
        dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
        dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
        dplyr::summarise(value=sum(value)) %>% 
        dplyr::ungroup(.)
    
    ,
    us, us %>% 
        dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
        dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
        dplyr::summarise(value=sum(value)) %>% 
        dplyr::ungroup(.)
    
    ,
    um, um %>% 
        dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
        dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
        dplyr::summarise(value=sum(value)) %>% 
        dplyr::ungroup(.)
    
    ,
    sum, sum %>% 
          dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
          dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
          dplyr::summarise(value=sum(value)) %>% 
          dplyr::ungroup(.)
    ,
    bm, bm %>% 
        dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
        dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
        dplyr::summarise(value=sum(value)) %>% 
        dplyr::ungroup(.)
   
    ,
    sbm, sbm %>% 
          dplyr::filter(., as.numeric(substr(time_view, 5,6)) <= month(bssdt)) %>% # filter data for ytd calculation
          dplyr::group_by(time_view=substr(time_view, 1, 4), territory, kpi, idx) %>% 
          dplyr::summarise(value=sum(value)) %>% 
          dplyr::ungroup(.)
    
  ) 
  
  mp = mp_group_by_territory %>% 
    tidyr::spread(., time_view, value)
  
  mp %>% 
    dplyr::mutate(level='TERRITORY', province='', region='', zone='', name=territory) %>% 
    dplyr::arrange(., territory, idx) %>% 
    fill_excel_column1.2(
      df=.,
      sheetname="Ending MP_Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 4,
      template=excelFile,
      result_file=excelFile,
      "#CCFFFF"
    )
  # country
  mp_group_by_territory %>% dplyr::group_by(time_view, kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='COUNTRY', province='', region='', zone='', name='', territory='') %>% 
    dplyr::arrange(., idx) %>% 
    fill_excel_column1.2(
      df=.,
      sheetname="Ending MP_Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 4+nrow(mp),
      template=excelFile,
      result_file=excelFile,
      "#00CCFF"
    )
  
  # Production_AD Structure -------------------------------------------------
  data_group_by_territory = Production_AD_Structure_REGION(bssdt)
  
  #
  # Production_AD Structure: territory
  # 
  dt1 = data_group_by_territory %>% 
    tidyr::spread(., time_view, value)
  dt1 %>% 
    dplyr::mutate(level='TERRITORY') %>% 
    dplyr::mutate(province='', region='', zone='', name=territory) %>% 
    dplyr::arrange(., territory, idx) %>% 
    fill_excel_column1.3(
      df=.,
      sheetname="Production_AD Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 4,
      template=excelFile,
      result_file=excelFile,
      "#CCFFFF"
    )
  # 
  # Production_AD Structure: country
  #
  Production_AD_Structure_COUNTRY(bssdt) %>% 
    dplyr::group_by(time_view, kpi, idx, fmt) %>% dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='COUNTRY', province='', region='', zone='', name='', territory='') %>% 
    dplyr::arrange(., idx) %>% 
    fill_excel_column1.3(
      df=.,
      sheetname="Production_AD Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 4+nrow(dt1),
      template=excelFile,
      result_file=excelFile,
      "#00CCFF"
    )
  
  # Recruitment_Structure ---------------------------------------------------
  dt_group_by_region <- Recruitment_Structure_REGION(bssdt) %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='TERRITORY') %>% 
    dplyr::mutate(province='', region='', zone='', name=territory) %>% 
    dplyr::arrange(., territory, idx)
  fill_excel_column1.3(
      df=dt_group_by_region,
      sheetname="Recruitment_Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 4,
      template=excelFile,
      result_file=excelFile,
      "#CCFFFF"
    )
  # group by country
  Recruitment_Structure_REGION(bssdt) %>% 
    dplyr::group_by(time_view, kpi, idx, fmt) %>% dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='COUNTRY', province='', region='', zone='', name='', territory='') %>% 
    dplyr::arrange(., idx) %>% 
    fill_excel_column1.3(
      df=.,
      sheetname="Recruitment_Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 4+nrow(dt_group_by_region),
      template=excelFile,
      result_file=excelFile,
      "#00CCFF"
    )
}









create_report(as.Date('2017-08-31'))
rm(list=ls())


# end segment -------------------------------------------------------------


