source('KPI_PRODUCTION/Common functions.R')

business_date = as.Date('2017-06-30')

agent_retention(business_date) %>% insert_or_replace_bulk(., 'report_agent_retention')

total_recruit_mtd(business_date) %>% insert_or_replace_bulk(., 'report_agent_retention')

for (business_date in generate_last_day_of_month(2016)) {
  promoted_AL(as.Date(business_date)) %>% insert_or_replace_bulk(., 'report_kpi_segmentation')
}
for (business_date in generate_last_day_of_month(2017)) {
  demoted_AL(as.Date(business_date)) %>% insert_or_replace_bulk(., 'report_kpi_segmentation')
}
for (business_date in generate_last_day_of_month(2017)) {
  terminated_agents(as.Date(business_date)) %>% insert_or_replace_bulk(., 'report_kpi_segmentation')
}
for (business_date in generate_last_day_of_month(2017)) {
  get_segmentation_by_genlion_sa_rookie(as.Date(business_date)) %>% insert_or_replace_bulk(., 'report_kpi_segmentation')
}

# get_segmentation_by_genlion_sa_rookie(as.Date('2017-04-30')) 
# get_segmentation_by_genlion_sa_rookie(as.Date('2017-05-31')) 
# terminated_agents(as.Date('2017-05-31'))


