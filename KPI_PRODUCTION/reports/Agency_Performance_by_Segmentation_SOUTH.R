source('KPI_PRODUCTION/Common functions.R')


South <-read_excel("D:/workspace_r/gvl_data_utilities/KPI_PRODUCTION/output/Agency Performance by Segmentation/Agency Performance by Segmentation_2015_201707.xlsx", 
                   sheet = "Agency South")

# remove rows that have no value in the columm 1
South <- South[!(is.na(South[,1])),]
# remove columns that have no column names
South <- South[,!(is.na(colnames(South)))]

t1=tidyr::gather(South, time_view, value, -kpi)
t1[,'territory']='SOUTH'
# extract the last 2 digits of a string of characters in R
t1[,'yy']=sub(".*(\\d+{2}).*$", "\\1", t1$time_view)

# save to database
insert_or_replace_bulk(t1, 'report_kpi_segmentation')
