source('KPI_PRODUCTION/Common functions.R')
source('KPI_PRODUCTION/reports/MONTHLY_AGENCY_SEGMENTATION_REPORT/agency_seg_man_power.R')

update_kpi_segmentation <- function() {
  bssdt = as.Date('2017-11-30')
  genlion_final_dt = as.Date('2017-09-30')
  dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database.db'
  #----
  get_Rookie_Performance(bssdt) %>%  dplyr::mutate(level='TERRITORY') %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Case_per_Active(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Case(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  CaseSize(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  # Activity_Ratio_1.1---
  # Activity_Ratio(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  Activity_Ratio_1.1(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  Activity_Ratio_1.2(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  Activity_Ratio_1.3_TOTAL(as.Date('2017-08-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Active(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  Active1.1(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  
  #----
  Recruitment(bssdt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  APE(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  RYP(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  FYP(bssdt, genlion_final_dt, dbfile) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Ending_MP(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #----
  Manpower(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
  #---
  product_mix_calculation(fromdt = as.Date('2016-01-01'), bssdt) %>% insert_or_replace_bulk(., 'product_mix')
  #---
  agent_retention(bssdt) %>% dplyr::mutate(territory='') %>%  insert_or_replace_bulk(., 'report_agent_retention')
  #---
  Rider(bssdt) %>%  insert_or_replace_bulk(., 'kpi_segmentation')
}

for (bsdt in generate_last_day_of_month(2016)) {
  APE_exclude_GA_for_meeting_purpose_only(as.Date(bsdt), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
}

for (bsdt in generate_last_day_of_month(2016)) {
  APE_GAHCM2_GATIENGIANG1_OTHERS(as.Date(bsdt), dbfile) %>% dplyr::select(-SEG_DESC) %>%  insert_or_replace_bulk(., 'kpi_segmentation')
}
for (bsdt in generate_last_day_of_month(2017)) {
  Manpower_GAHCM2_GATIENGIANG1_OTHERS(as.Date(bsdt), dbfile) %>% dplyr::select(-SEG_DESC) %>%  insert_or_replace_bulk(., 'kpi_segmentation')
}

Manpower_GAHCM2_GATIENGIANG1_OTHERS(as.Date("2017-10-31"), dbfile) %>%  insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_GAHCM2_GATIENGIANG1_OTHERS(as.Date("2017-11-30"), dbfile) %>%  insert_or_replace_bulk(., 'kpi_segmentation')

APE_GAHCM2_GATIENGIANG1_OTHERS(as.Date("2017-10-31"), dbfile) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_GAHCM2_GATIENGIANG1_OTHERS(as.Date("2017-11-30"), dbfile) %>% insert_or_replace_bulk(., 'kpi_segmentation')


APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-01-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-02-28'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-03-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-04-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-05-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-06-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-07-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-08-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-09-30'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-10-31'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
APE_exclude_GA_for_meeting_purpose_only(as.Date('2017-11-30'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

APE_exclude_GA_for_meeting_purpose_only(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')

#----Active1.1_exclude_2GA
Active1.1_exclude_2GA(as.Date('2017-01-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-02-28'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-03-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Active1.1_exclude_2GA(as.Date('2017-04-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-05-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-06-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Active1.1_exclude_2GA(as.Date('2017-07-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-08-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-09-30'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-10-31'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Active1.1_exclude_2GA(as.Date('2017-11-30'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Active1.1_exclude_2GA(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
#----Manpower_exclude_2GA
Manpower_exclude_2GA(as.Date('2017-01-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-02-28'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-03-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Manpower_exclude_2GA(as.Date('2017-04-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-05-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-06-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Manpower_exclude_2GA(as.Date('2017-07-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-08-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-09-30'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-10-31'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Manpower_exclude_2GA(as.Date('2017-11-30'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Manpower_exclude_2GA(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
#----CaseSize_exclude_2GA
CaseSize_exclude_2GA(as.Date('2017-01-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-02-28'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-03-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

CaseSize_exclude_2GA(as.Date('2017-04-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-05-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-06-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

CaseSize_exclude_2GA(as.Date('2017-07-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-08-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-09-30'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-10-31'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
CaseSize_exclude_2GA(as.Date('2017-11-30'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

CaseSize_exclude_2GA(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')
#----Case_per_Active_exclude_2GA
Case_per_Active_exclude_2GA(as.Date('2017-01-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-02-28'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-03-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Case_per_Active_exclude_2GA(as.Date('2017-04-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-05-31'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-06-30'), as.Date('2017-03-31')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Case_per_Active_exclude_2GA(as.Date('2017-07-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-08-31'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-09-30'), as.Date('2017-06-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-10-31'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')
Case_per_Active_exclude_2GA(as.Date('2017-11-30'), as.Date('2017-09-30')) %>% insert_or_replace_bulk(., 'kpi_segmentation')

Case_per_Active_exclude_2GA(bssdt, genlion_final_dt) %>% insert_or_replace_bulk(., 'kpi_segmentation')

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
     dplyr::mutate(level='TERRITORY')
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
     dplyr::mutate(level='TERRITORY')
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
  dplyr::mutate(level='TERRITORY')
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
  dplyr::mutate(level='TERRITORY')
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
# update data -------------------------------------------------------------
#
for (bsdt in generate_last_day_of_month(2015)) {
  rs <- active_recruit_leader(as.Date(bsdt)) %>% dplyr::mutate(level='TERRITORY') %>% # active recruit leader: us, um, bm
  insert_or_replace_bulk(., 'kpi_segmentation')
}






#
# create report -----------------------------------------------------------
#
create_report <- function(bssdt, exceltemplate, excelFile){
  # Sheet Country -----------------------------------------------------------
  sheetname = 'Country'
  t2=kpi_segmentation(criteria = "where level='COUNTRY' and (territory is null or territory='' or territory='COUNTRY')")
  # t2[,'value'] <- as.numeric(t2[,'value'],)
  t3=tidyr::spread(t2, time_view, value)
  # set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
  rownames(t3)<-gsub("^\\s+|\\s+$", "", t3[,'kpi'])
  
  replace_cellvalue2.1(t3,
                       sheetname=sheetname,
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=exceltemplate,
                       result_file=excelFile)
  
  # 
  # sheet North -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  #
  
  t2=kpi_segmentation(criteria = "where territory='NORTH' and level='TERRITORY'")
  # t2[,'value'] <- as.numeric(t2[,'value'],)
  t3=tidyr::spread(t2, time_view, value)
  # set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
  rownames(t3)<-gsub("^\\s+|\\s+$", "", t3[,'kpi'])
  
  replace_cellvalue2.1(t3,
                       sheetname="North",
                       rowNameColIndex = 1,
                       headerRowIndex = 1,
                       template=excelFile,
                       result_file=excelFile)
  
  
  #
  # sheet South -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  #
  
  t2=kpi_segmentation(criteria = "where territory='SOUTH' and level='TERRITORY'")
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
 
  
  # 5.0 AG retention --------------------------------------------------------
  # retrieve active agent only
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
  
  newrecruit_agents_ter_incl =  new_recruit_individual(bssdt, dbfile) 

  newrecruit_agents_ter_incl_country = count_group_by(df=newrecruit_agents_ter_incl, f1='time_view', summarise_field_name='NEWRECRUIT') %>% 
    dplyr::filter(group_level=='COUNTRY') %>% 
    dplyr::mutate(kpi = paste('agent_retention:recruited_in_', time_view, sep = '')) %>% 
    dplyr::mutate(time_view = 'new_crecuted_inmonth') %>% 
    tidyr::spread(time_view, NEWRECRUIT)
   # set rownames equal to column1: trim leading and trailing whitespace (left trim, right trim) 
  rownames(newrecruit_agents_ter_incl_country)<-gsub("^\\s+|\\s+$", "", newrecruit_agents_ter_incl_country[,'kpi'])
  replace_cellvalue2.1(newrecruit_agents_ter_incl_country,
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
  
  # Product_mix -------------------------------------------------------------
  t1 = product_mix(criteria = "WHERE product_code not in ('RIDER', 'SME')", orderby = strftime(bssdt, '%Y')  ,addTotalRow = T)
  fill_excel_column1.3(
    df=t1 %>% dplyr::filter(product_code != 'Basic Total'),
    sheetname="Product Mix",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 8,
    template=excelFile,
    result_file=excelFile
  )
  
  t2=rbind(
    t1 %>% dplyr::filter(product_code == 'Basic Total'),
    product_mix(criteria = "WHERE product_code in ('RIDER', 'SME')", orderby = strftime(bssdt, '%Y')) %>% 
      dplyr::mutate(Total = 0)
  )
  t2$product_name=''
  fill_excel_column1.3(
    df=t2,
    sheetname="Product Mix",
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
    sheetname="Product Mix",
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
      sheetname="Product Mix",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 8+nrow(t1)+nrow(t2)+1,
      template=excelFile,
      result_file=excelFile,
      '#CC99FF'
    )
  
  # GA performance ----------------------------------------------------------------
  GA_per = GA(bssdt)
  GA_per = GA_per[order(GA_per[sprintf('%s_APE', strftime(bssdt, '%Y%m'))], decreasing = T),]
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
 
  
# Rookie Metric -----------------------------------------------------------
 
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
  
  fill_excel_column1.2(
    df=dt,
    sheetname="Rookie Metric",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = 4,
    template=excelFile,
    result_file=excelFile,
    color_territory
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
  write_from = 4
  mp_group_by_territory = Ending_MP_Structure(bssdt) 
  
  mp = mp_group_by_territory %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='TERRITORY', province='', region='', zone='', name=territory, fmt='#,##0') %>% 
    dplyr::arrange(., territory, idx)
  write_from = fill_excel_column1.3(
      df=mp,
      sheetname="Ending MP_Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = 4,
      template=excelFile,
      result_file=excelFile,
      color_territory
    )
  # country
  tempdt = mp_group_by_territory %>% dplyr::group_by(time_view, kpi, idx) %>% 
    dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='COUNTRY', province='', region='', zone='', name='', territory='', fmt='#,##0') %>% 
    dplyr::arrange(., idx)
  
  write_from = fill_excel_column1.3(
      df=tempdt,
      sheetname="Ending MP_Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = write_from,
      template=excelFile,
      result_file=excelFile,
      "#00CCFF"
    )
  
  # Production_AD Structure -------------------------------------------------
  write_from = 4
  # prd_ad_re = Production_AD_Structure_REGION(bssdt) %>% 
  #   tidyr::spread(.,time_view, value) %>% 
  #   dplyr::mutate(province='', zone='', name=territory) %>% 
  #   dplyr::arrange(., territory, idx)
  # 
  # write_from = fill_excel_column1.3(
  #     df=prd_ad_re,
  #     sheetname="Production_AD Structure",
  #     rowNameColIndex = 1,
  #     headerRowIndex = 1,
  #     start_writing_from_row = write_from,
  #     template=excelFile,
  #     result_file=excelFile,
  #     "#FFFF99"
  #   )
  
  #
  # Production_AD Structure: territory
  # 
  data_group_by_territory = Production_AD_Structure_TERRITORY(bssdt)
  dt1 = data_group_by_territory %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='TERRITORY') %>% 
    dplyr::mutate(province='', region='', zone='', name=territory) %>% 
    dplyr::arrange(., territory, idx) 
    
  write_from =  fill_excel_column1.3(
      df=dt1,
      sheetname="Production_AD Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = write_from,
      template=excelFile,
      result_file=excelFile,
      color_territory
    )
  # 
  # Production_AD Structure: country
  #
  temdf = Production_AD_Structure_COUNTRY(bssdt) %>% 
    dplyr::group_by(time_view, kpi, idx, fmt) %>% dplyr::summarise(value=sum(value)) %>% 
    dplyr::ungroup(.) %>% 
    tidyr::spread(., time_view, value) %>% 
    dplyr::mutate(level='COUNTRY', province='', region='', zone='', name='', territory='') %>% 
    dplyr::arrange(., idx) 
  
  write_from = fill_excel_column1.3(
      df=temdf,
      sheetname="Production_AD Structure",
      rowNameColIndex = 1,
      headerRowIndex = 1,
      start_writing_from_row = write_from,
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
      color_territory
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
  
  # Recruitment KPI_Structure -----------------------------------------------
  write_from = 4
  tem = Recruitment_KPI_Structure_TERRITORY(bssdt) %>% 
    tidyr::spread(time_view, value) %>% 
    dplyr::arrange(territory, idx) %>% 
    dplyr::mutate(province='', region='', zone='', name='')
  write_from =  fill_excel_column1.3(
    df=tem,
    sheetname="Recruitment KPI_Structure",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template=excelFile,
    result_file=excelFile,
    color_territory
  )
  tem = Recruitment_KPI_Structure_COUNTRY(bssdt) %>% 
    tidyr::spread(time_view, value) %>% 
    dplyr::arrange(idx) %>% 
    dplyr::mutate(province='', region='', zone='', name='', territory='')
  write_from =  fill_excel_column1.3(
    df=tem,
    sheetname="Recruitment KPI_Structure",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template=excelFile,
    result_file=excelFile,
    "#00CCFF"
  )
  # Rider sheet -------------------------------------------------------------
  write_from = 4
  dt = Rider_sheet(bssdt)
  
  write_from = fill_excel_column1.4(
    df=dt,
    sheetname="Rider",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = write_from,
    template=excelFile,
    result_file=excelFile
  )
}












# MAIN PROCESS ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if(interactive()) {
  bssdt = as.Date('2017-11-30')
  bssdt_m1 = get_last_day_of_m1(strftime(bssdt, '%Y'), strftime(bssdt, '%m'))
 
  # excelFile = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_SEGMENTATION_REPORT_%s.xlsx", strftime(bssdt,'%Y-%m-%d'))
  # exceltemplate = sprintf("d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\MONTHLY_AGENCY_SEGMENTATION_REPORT_%s.xlsx", strftime(bssdt_m1,'%Y-%m-%d'))
  # exceltemplate = "d:\\workspace_excel\\GVL\\DOMS\\Agency Monthly Report\\template_MONTHLY_AGENCY_SEGMENTATION_REPORT_2017-10-11.xlsx"
  
  # current folder 
  excelFile = sprintf("KPI_PRODUCTION/reports/MONTHLY_AGENCY_SEGMENTATION_REPORT/MONTHLY_AGENCY_SEGMENTATION_REPORT_%s.xlsm", strftime(bssdt,'%Y-%m-%d'))
  exceltemplate = sprintf("KPI_PRODUCTION/reports/MONTHLY_AGENCY_SEGMENTATION_REPORT/MONTHLY_AGENCY_SEGMENTATION_REPORT_%s.xlsm", strftime(bssdt_m1,'%Y-%m-%d'))
  
  create_report(bssdt, exceltemplate, excelFile)
  
  agency_man_power = get_agency_man_power(bssdt)
  agency_man_power_country = agency_man_power %>% dplyr::filter(segment == 'COUNTRY') 
  agency_man_power_north = agency_man_power %>% dplyr::filter(segment == 'NORTH') 
  agency_man_power_south = agency_man_power %>% dplyr::filter(segment == 'SOUTH') 
  
  # Segment Manpower 
  row = 76
  write_from = fill_excel_column1.4_no_fmt(
    df = agency_man_power_country,
    sheetname = "Country",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = row,
    template = excelFile,
    result_file = excelFile
  ) 
  write_from = fill_excel_column1.4_no_fmt(
    df = agency_man_power_north,
    sheetname = "North",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = row,
    template = excelFile,
    result_file = excelFile
  ) 
  write_from = fill_excel_column1.4_no_fmt(
    df = agency_man_power_south,
    sheetname = "South",
    rowNameColIndex = 1,
    headerRowIndex = 1,
    start_writing_from_row = row,
    template = excelFile,
    result_file = excelFile
  ) 
  
  
  group_persistency = rbind(get_segment_persistency(bssdt), get_group_persistency(bssdt))
  group_persistency %>% 
    dplyr::filter(segment == 'COUNTRY' | level == 'COUNTRY') 
  
  
  group_persistency %>% 
    dplyr::filter(segment == 'NORTH' | level == 'NORTH') 
  
  group_persistency %>% 
    dplyr::filter(segment == 'SOUTH' | level == 'SOUTH')
  # rm(list=ls())
}




