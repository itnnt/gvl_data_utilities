source('EDM_set_common_input.R')

#1__ CHANNEL STRUCTURE ####
#__1 CHANNEL SETUP ####
#(list down all channels in the system)####
channel <- dplyr::tbl_df(get_channel_setup(env)) #%>% select(CHANNELCD, CHANNELNAME1, PARENTCHANNELCD)
subchannels <- dplyr::tbl_df(get_subchannel(env)) #%>% select(CHANNELCD, CHANNELNAME1, PARENTCHANNELCD) 
# AGENCY AND PRODUCER ####
View(get_all_agencies(env))
get_producer_info(env, 'BD000049')
get_producer_info(env, '2950')

# PRODUCT: CÁC SẢN PHẨM ĐANG BÁN  ####
get_product(env, "PRODUCTCD"="('CIB2')")
get_product_group_detail(env, "PRODUCTCD"="('CIB2')")
get_product_group(env, "PRODUCTGROUPCD"="('COMCIB2')")
get_product_group(env, "PRODUCTGROUPCD"="('PRODUCTIONALL')")
get_product_group(env, "PRODUCTGROUPCD"="('ENDOWMENTLTDPAY')")


# kpi -------------------------------------------------------------------------------
get_kpi_mapping_to_channel(env)

# group kpi mapping to channel ------------------------------------------------------
get_kpi_goup(env, 'TMPHANCLUB')
get_kpi_goup_subordinate(env, 'GROUPCD'="( 'TTCMG','TBCMG','TCCMG')" )

get_kpi_goup_subordinate(env, 'GROUPCD'="('BKOCB', 'BROCB', 'TOOCB')" )
get_kpi_goup_subordinate(env, 'GROUPCD'="( 'TOOCB')" )
get_kpi_goup_subordinate('COMPANY_EDM_VN', 'GROUPSUBORDINATESEQ'="( '307')" )
get_kpi_goup_subordinate('COMPANY_EDM_VN_UAT6', 'GROUPSUBORDINATESEQ'="( '307')" )
get_kpi_goup_detail(env, 'TCCMG')

# get incentive ---------------------------------------------------------------------
get_incentive_definition(env, 'ULPBONUS1')
get_incentive_result(env, 'CB.BENEFITEVALDT'="('2017-02-28')", "CB.BENEFITCD" = "('IBWHP150801BTCBCOMA')")
get_incentive_result(env, '2017-02-28', "CPM.PRODUCERSOURCECD" = "'AG013119'")
get_incentive_result(env,
                     '2017-02-27',
                     "CB.BENEFITCD" = "'IBWHP150801BTCBCOMA'",
                     "ADHOCFL" = "'NO'")
get_incentive_result_kpis(env, '2017-03-05')

# policy ----------------------------------------------------------------------------
#_ get all policies----
allpolicies <- get_policy(env)
names(allpolicies)

#_ select some columns----
po_info_for_user <-
  dplyr::select(allpolicies, POLICYSEQ, POLICYCD, ACKNOWLEDGEMENTDT)
#_ save policies to xlsx file----
xlsx_write_to_file(po_info_for_user, paste(output, "po_info_for_user", sep =
                                             '/'))

#_ get policy info----
get_policy(env, 60032757)

#_ get policy premium----
premiums <- get_policy_premium(env, 60032757)

#_ get policy products----
prd <- get_policy_product(env, 60032757)
xlsx_write_to_file_multi_sheets(premiums,
                                "S:/IT/Share to others/tung/DFUWF-734/DFU_CheckDay_20170125",
                                'premiums')

# dfu kpi ---------------------------------------------------------------------------
kpi_sum <-
  read.xlsx(file.path(output, 'DFUWF-734', 'DFU_CheckDay_20170125.xlsx'),
            sheetName = 'KPI_SUM')
kpi_detail <-
  read.xlsx(file.path(output, 'DFUWF-734', 'DFU_CheckDay_20170125.xlsx'),
            sheetName = 'KPI_DETAIL')

personal <-
  read.xlsx(
    file.path(
      output,
      'DFUWF-734',
      'TMINHOUSE_TMPHANCLUB_PRODUCTION_DATA_COMPANY_EDM_VN_DR_2017-01-25.xlsx'
    ),
    sheetName = 'Personal'
  )

# Left outer
merge(
  x = kpi_sum,
  y = personal,
  by.x = c("AGCode", "KPI", "ProductCode"),
  by.y = c("producersourcecd", "PRODNUNITCD", "PRODUCTCD"),
  all.x = TRUE
)


# dfu payable TIEDAGENCY ------------------------------------------------------------
payables <- get_payable(env, '2017-03-01', '2017-03-15', 'INHOUSE')
#_ extract payable whose paytoproducercd diff to sourceproducercd----
xlsx_write_to_file(payables[payables$SOURCEPRODUCERCD != payables$PAYTOPRODUCERCD,], file.path(output, 'PAYABLE 201702 SOURCEPRODUCERCD DIFF TO PAYTOPRODUCERCD', 'payable_201702'))

# dfu production --------------------------------------------------------------------
# read file update datapatch
# df <- read.table(file.path(output,'DFUWF-740','Break', 'BH_DFUWF-740_KPIs_Sum.sql'), 
#                  sep="\t", 
#                  strip.white=TRUE)
update_production_summary(env, "UPDATE CMS_PRODUCTION_SUMMARY SET PRODNVALUE=0 WHERE PRODNSUMMSEQ=559722900 AND PRODUCERCD ='56' AND  PRODUCTCD='WOP1' AND PRODNUNITCD='4YP'  --AGENTCD= AG000008 PRODNVALUE = 354000")
# dfu execute patch file ------------------------------------------------------------
execute_sqlfile(file.path(output, 'DFUWF-741', 'Break', 'TUNG_DFU_CheckDay_20170228____sumonly.sql'),env)

get_production_summary_tbl(env, 'PRODNSUMMSEQ'='(561021790)')
get_production_summary_detail_tbl(env, 'TRANSACTIONDT'="('2017-03-02')", 'PRODUCERCD'="(37478)")
execute_sqlfile(file.path(output, 'DFUWF-740', 'SCRIPT_LOG_.20170228.20170302-182348.sql'),env)


# analyzing dfu file ----------------------------------------------------------------
KPI_DETAIL <- dplyr::tbl_df(read.xlsx(file.path(output, 'DFUWF-740', 'DFU_CheckDay_20170228_KPIs_UAT_v3.xlsx'), sheetName = 'KPI_DETAIL'))
#_ format user's field names----
field_names <- names(KPI_DETAIL)
field_names <- gsub('LeaderCode', 'LEADERCD', field_names)
#_rename data frame
names(KPI_DETAIL) <- field_names

KPI_SUM <- dplyr::tbl_df(read.xlsx(file.path(output, 'DFUWF-740', 'DFU_CheckDay_20170228_KPIs_UAT_v3.xlsx'), sheetName = 'KPI_SUM'))


Personal <- read_excel(file.path(output, 'DFUWF-740-data-patch', 'TIEDAGENCY_PRODUCTION_DATA_EDM_UAT6_2017-02-28.xlsx'), sheet='Personal')
Group_detail <- read_excel(file.path(output, 'DFUWF-740-data-patch', 'TIEDAGENCY_PRODUCTION_DATA_COMPANY_EDM_VN_DR_2017-02-28.xlsx'), sheet='Group_detail')

# inner join
mergedt <- merge(
  x = KPI_SUM,
  y = Personal,
  by.x = c("AGENTCD", "KPICODE", "PRODUCTCD"),
  by.y = c("producersourcecd", "PRODNUNITCD", "PRODUCTCD"),
  # all.x = TRUE
)

mergedt <- merge(
  x = KPI_DETAIL,
  y = Group_detail,
  by.x = c("LEADERCD", "AGENTCD", "PRODUCTCD", "KPICODE", "GROUPCD", "POLICYCD"),
  by.y = c("MAINPRODUCERSOURCECD", "SUBPRODUCERSOURCECD", "productcd", "PRODNUNITCD", "MAINPRODNGROUPCD", "SUBPOLICYSOURCECD"),
  # all.x = TRUE
)
xlsx_write_to_file(mergedt, file.path(output, 'DFUWF-740-data-patch', 'incorrect_detail'))
get_production_summary(env, 'PRODNSUMMSEQ'=sprintf("(%s)", paste(mergedt$PRODNSUMMSEQ, collapse = ',')))

xlsx_write_to_file(mergedt, file.path(output, 'DFUWF-740-data-patch', 'incorrect_detail'))

