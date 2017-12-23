source('KPI_PRODUCTION/Common functions.R')

banca_performance_function_update <- function(bssdt, sourcefile) {
  
  sheet1 <- read_excel(sourcefile, sheet = 1)  
  fieldnames <- names(sheet1) # get all field names in the result
  fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub("[\r\n]", "", fieldnames)
  names(sheet1) <- toupper(fieldnames) # rename all fields in the result
  sheet1 = sheet1 %>% 
    dplyr::select(OFFICE_NAME, TEAM_NAME, AGENT_CODE, AGENT_FULL_NAME, AGENT_DESIGNATION) 
    # dplyr::mutate(JOINING_DATE = convertToDate(JOINING_DATE))
  
  #--
  sheet2 <- read_excel(sourcefile, sheet = 2)  
  fieldnames <- names(sheet2) # get all field names in the result
  fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub("[\r\n]", "", fieldnames)
  names(sheet2) <- toupper(fieldnames) # rename all fields in the result
  sheet2 = sheet2 %>% 
    dplyr::select(OFFICE_NAME, TEAM_NAME, AGENT_CODE, AGENT_FULL_NAME, AGENT_DESIGNATION) 
    # dplyr::mutate(JOINING_DATE = convertToDate(JOINING_DATE))
  #--
  sheet3 <- try((read_excel(sourcefile, sheet = 3)), silent = T)
  fieldnames <- names(sheet3) # get all field names in the result
  fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
  fieldnames <- gsub("[\r\n]", "", fieldnames)
  names(sheet3) <- toupper(fieldnames) # rename all fields in the result
  sheet3 = sheet3 %>% 
    dplyr::select(OFFICE_NAME, TEAM_NAME, AGENT_CODE, AGENT_FULL_NAME, AGENT_DESIGNATION)
    # dplyr::mutate(JOINING_DATE = convertToDate(JOINING_DATE))
      
  
  
  mp = rbind(
    sheet1,
    sheet2,
    sheet3
  ) %>% 
    # dplyr::filter(!is.na(JOINING_DATE)) %>% 
    dplyr::mutate(BSSDT = strftime(bssdt, '%Y-%m-%d'))
  
  insert_or_replace_bulk(mp, 'BANCASSURANCE_MANPOWER', dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database_BANCASSURANCE.db') # save to database
  
  
  
  
}
banca_performance_function_create_report <- function(bssdt, excelFile, exceltemplate){
  businessdate = bssdt
  office_ape <- get_ape_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
    tidyr::spread(., time_view, APE)
  rownames(office_ape) <- paste('APE', office_ape$OFFICE, sep = '_')  
  sheet = 'Data'
  replace_cellvalue2.1(
    office_ape,
    sheetname = sheet,
    rowNameColIndex = 1,
    headerRowIndex = 2,
    template = exceltemplate,
    result_file = excelFile
  )
  
  office_rider_ape <- get_rider_ape_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
    tidyr::spread(., time_view, APE)
  rownames(office_rider_ape) <- paste('APE_RIDER', office_rider_ape$OFFICE, sep='_') 
  sheet = 'Data'
  replace_cellvalue2.1(
    office_rider_ape,
    sheetname = sheet,
    rowNameColIndex = 1,
    headerRowIndex = 2,
    template = excelFile,
    result_file = excelFile
  )
  
  office_main_case <- get_main_case_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
    tidyr::spread(., time_view, CASECOUNT)
  rownames(office_main_case) <- paste('CASECOUNT', office_main_case$OFFICE, sep = '_') 
  sheet = 'Data'
  replace_cellvalue2.1(
    office_main_case,
    sheetname = sheet,
    rowNameColIndex = 1,
    headerRowIndex = 2,
    template = excelFile,
    result_file = excelFile
  )
  
  office_rider_case <- get_rider_case_sum_by_office(last_day_of_months = businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
    tidyr::spread(., time_view, CASECOUNT)
  rownames(office_rider_case) <- paste('CASECOUNT_RIDER', office_rider_case$OFFICE, sep = '_') 
  sheet = 'Data'
  replace_cellvalue2.1(
    office_rider_case,
    sheetname = sheet,
    rowNameColIndex = 1,
    headerRowIndex = 2,
    template = excelFile,
    result_file = excelFile
  )
  
  
  office_casesize <- get_office_casesize(businessdate, 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
    tidyr::spread(., time_view, CASESIZE)
  rownames(office_casesize) <- paste('CASESIZE', office_casesize$OFFICE, sep = '_') 
  sheet = 'Data'
  replace_cellvalue2.1(
    office_casesize,
    sheetname = sheet,
    rowNameColIndex = 1,
    headerRowIndex = 2,
    template = excelFile,
    result_file = excelFile
  )
  
  office_ape_group_designation <- get_ape_sum_by_office_designation(businessdate, dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
    tidyr::spread(., time_view, APE)
  rownames(office_ape_group_designation) <- paste('APE', paste(office_ape_group_designation$OFFICE, office_ape_group_designation$AGCODEDESIGNATION, sep = ' '), sep = '_') 
  sheet = 'Data'
  replace_cellvalue2.1(
    office_ape_group_designation,
    sheetname = sheet,
    rowNameColIndex = 1,
    headerRowIndex = 2,
    template = excelFile,
    result_file = excelFile
  )  
  
  mp = get_BANCA_mp(dbfile = 'KPI_PRODUCTION/main_database_BANCASSURANCE.db') %>% 
    dplyr::group_by(OFFICE, DESIGNATIONCODE, time_view) %>% 
    dplyr::summarise(MP = n()) %>% 
    tidyr::spread(., time_view, MP)
  rownames(mp) <- paste('MP', paste(mp$OFFICE, mp$DESIGNATIONCODE, sep = ' '), sep = '_') 
  sheet = 'Data'
  replace_cellvalue2.1(
    mp,
    sheetname = sheet,
    rowNameColIndex = 1,
    headerRowIndex = 2,
    template = excelFile,
    result_file = excelFile
  )
}
banca_performance_function_update_v1 <- function(bssdt, sourcefile) {
  sheet_num = length(readxl::excel_sheets( sourcefile ) )
  # apply-like function that returns a data frame
  mp = do.call(rbind,lapply(c(1:sheet_num), function(idx) {
    sheet2 <- (read_excel(sourcefile, sheet = idx)  )
    fieldnames <- names(sheet2) # get all field names in the result
    fieldnames <- gsub(' ', '_', fieldnames) # replace all special characters in the fieldnames by '_'
    fieldnames <- gsub('%', '', fieldnames) # replace all special characters in the fieldnames by '_'
    fieldnames <- gsub('&', '', fieldnames) # replace all special characters in the fieldnames by '_'
    fieldnames <- gsub("[\r\n]", "", fieldnames)
    names(sheet2) <- toupper(fieldnames) # rename all fields in the result
    sheet2 = sheet2 %>%
      dplyr::filter(is.na(X__1) | toupper(X__1) == toupper("Teamlead")) %>% 
      dplyr::select(OFFICE_NAME, TEAM_NAME, AGENT_CODE, AGENT_FULL_NAME, AGENT_DESIGNATION) 
    sheet2
  })
  ) %>%  dplyr::mutate(BSSDT = strftime(bssdt, '%Y-%m-%d'))
  insert_or_replace_bulk(mp, 'BANCASSURANCE_MANPOWER', dbfile = 'D:\\workspace_data_processing\\gvl_data_utilities\\KPI_PRODUCTION\\main_database_BANCASSURANCE.db') # save to database
}
