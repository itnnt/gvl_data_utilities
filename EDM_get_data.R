source(paste(getwd(), '', 'EDM.R', sep = '/'))

env = 'COMPANY_EDM_VN'
# env = 'COMPANY_EDM_DAY2_22May2PM'
# env = 'COMPANY_EDM_VN_UAT6'
# env = 'COMPANY_EDM_UAT_VN'
# env = 'COMPANY_EDM_VN_DR'

# View(get_all_agencies(env))
# View(get_kpi_mapping_to_channel(env, 'TMPHANCLUB'))
# get_producer_info(env, 'AG000712')


# get incentive
View(get_incentive(env, '2017-03-05'))
View(get_incentive_kpis(env, '2017-03-05'))
