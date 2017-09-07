source('KPI_PRODUCTION/Common functions.R')

# NORTH SHEET -------------------------------------------------------------


df <-read_excel("d:\\workspace_excel\\GVL\\DOMS\\Agency Performance by Segmentation\\Agency Performance by Segmentation_2015_201707.xlsx", 
                    sheet = "Agency North")

# remove rows that have no value in the columm 1
df <- df[!(is.na(df[,1])),]
# remove columns that have no column names
df <- df[,!(is.na(colnames(df)))]
# remove columns that have prefix is "X__" (if any)
df <- dplyr::select(df, -grep("\\X__*", colnames(df)))

t1=tidyr::gather(df, time_view, value, -kpi)
t1[,'territory']='NORTH'
# extract the last 2 digits of a string of characters in R
# left 2 characters
t1[,'yy']=sub(".*(\\d+{2}).*$", "\\1", t1$time_view)

# save to database
insert_or_replace_bulk(t1, 'report_kpi_segmentation')


# SOUTH SHEET -------------------------------------------------------------


df <-read_excel("d:\\workspace_excel\\GVL\\DOMS\\Agency Performance by Segmentation\\Agency Performance by Segmentation_2015_201707.xlsx", 
                sheet = "Agency South")

# remove rows that have no value in the columm 1
df <- df[!(is.na(df[,1])),]
# remove columns that have no column names
df <- df[,!(is.na(colnames(df)))]
# remove columns that have prefix is "X__" (if any)
df <- dplyr::select(df, -grep("\\X__*", colnames(df)))

t1=tidyr::gather(df, time_view, value, -kpi)
t1[,'territory']='SOUTH'
# extract the last 2 digits of a string of characters in R
# left 2 characters
t1[,'yy']=sub(".*(\\d+{2}).*$", "\\1", t1$time_view)

# save to database
insert_or_replace_bulk(t1, 'report_kpi_segmentation')


# *** ---------------------------------------------------------------------


# GEN Lion ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
get_data_genlion_to_db <- function(Agency_Performance_by_Segmentation_xlsx, sheetname) {
  df <-read_excel(Agency_Performance_by_Segmentation_xlsx, 
                  sheet = sheetname)
  
  # remove rows that have no value in the columm 1
  df <- df[!(is.na(df[,1])),]
  df <- df[(df[,1]) !='',]
  # remove columns that have no column names
  df <- df[,!(is.na(colnames(df)))]
  df <- df[,""!=(colnames(df))]
  # remove columns that have prefix is "X__" (if any)
  df <- dplyr::select(df, -grep("\\X__*", colnames(df)))
  
  t1=tidyr::gather(df, time_view, value, -kpi)
  t1[,'territory']=sheetname
  # remove na values from t1
  t1 <- t1[!is.na(t1$value),]
  # replace all by empty except the first 4 chars
  # get the left 4 chars
  t1[,'yy']=sub("(^\\d{4}).*", "\\1", t1$time_view)
  
  # save to database
  insert_or_replace_bulk(t1, 'report_kpi_segmentation')
}

get_data_genlion_to_db(Agency_Performance_by_Segmentation_xlsx="d:\\workspace_excel\\GVL\\DOMS\\Agency Performance by Segmentation\\Agency Performance by Segmentation_2015_201707.xlsx", 
                       sheetname = "GEN Lion NORTH")
get_data_genlion_to_db(Agency_Performance_by_Segmentation_xlsx="d:\\workspace_excel\\GVL\\DOMS\\Agency Performance by Segmentation\\Agency Performance by Segmentation_2015_201707.xlsx", 
                       sheetname = "GEN Lion SOUTH")



