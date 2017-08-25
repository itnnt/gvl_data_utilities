# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Required libraries ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
library(dplyr)
library(RSQLite)
library(xlsx)

# *** ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Overall_KPI_performance -------------------------------------------
North_Overall_KPI_performance <- list(result_index,
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_ending_manpower_exsa), TERRITORY=='NORTH'), -c(TERRITORY)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_ending_manpower_sa), TERRITORY=='NORTH'), -c(TERRITORY)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_activity_ratio), TERRITORY=='NORTH'), -c(TERRITORY, `# Active`, ACTIVE_AGENTS_MONTHSTART, ACTIVE_AGENTS_MONTHEND)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_active_agents_ex), TERRITORY=='NORTH'), -c(TERRITORY)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_active_agents), TERRITORY=='NORTH'), -c(TERRITORY)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_cscnt), TERRITORY=='NORTH'), -c(TERRITORY)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_ape), TERRITORY=='NORTH'), -c(TERRITORY)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_case_count_4riders), TERRITORY=='NORTH'), -c(TERRITORY)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_persistency_k1), TERRITORY=='NORTH'), -c(TERRITORY, TOTALAPEORIGINAL_REGION, TOTALAPECURRENT_REGION)),
                                      dplyr::select(dplyr::filter(dplyr::ungroup(final_persistency_k2), TERRITORY=='NORTH'), -c(TERRITORY, TOTALAPEORIGINAL_REGION, TOTALAPECURRENT_REGION))
                                      
) %>%
  Reduce(function(dtf1,dtf2) merge(x=dtf1,y=dtf2,by.x = 'BUSSINESSDATE_FM1',
                                   by.y = 'BUSSINESSDATE' ,
                                   all.x = TRUE), ., accumulate=F) %>% 
  dplyr::mutate('# Case/active'=`#cases`/`# Active`) %>% 
  dplyr::mutate('Casesize'=APE/`#cases`) 

# Ending_MP ---------------------------------------------------------
North_Ending_MP <- list(
  result_index,
  dplyr::select(dplyr::filter(dplyr::ungroup(final_ending_manpower_total), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_ending_manpower_exsa), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_ending_manpower_sa), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_AG), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_US), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_UM), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_SUM), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_BM), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_SBM), TERRITORY=='NORTH'), -c(TERRITORY))
) %>% 
  Reduce(function(df1, df2) merge(x=df1, y=df2, by.x='BUSSINESSDATE_FM1', by.y='BUSSINESSDATE', all.x=TRUE), .) %>% 
  dplyr::select(-id) %>% 
  select(-c(BUSSINESSDATE))

# Recruitment ------------------------------------------------------
Recruitment <- list(
  result_index,
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_recruited_AL), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_New_recruits), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_New_recruits_AG), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_New_recruits_US), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_New_recruits_UM), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_New_recruits_SUM), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_New_recruits_BM), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Total_New_recruits_SBM), TERRITORY=='NORTH'), -c(TERRITORY))
) %>% 
  Reduce(function(df1, df2) merge(x=df1, y=df2, by.x='BUSSINESSDATE_FM1', by.y='BUSSINESSDATE', all.x=TRUE), .) %>% 
  dplyr::select(-id) %>% 
  select(-c(BUSSINESSDATE))
# Recruitment[,'Total recruited AL'] = rowSums(Recruitment[,c('US','UM','SUM','BM','SBM')], na.rm = T)
# # replase all 0 by NA
# Recruitment$`Total recruited AL`[Recruitment$`Total recruited AL`==0] <- NA

# reorder columns
Recruitment <- cbind(Recruitment[,c('BUSSINESSDATE_FM1', 'Total # New recruits', 'AG')],
      Recruitment[,c('Total recruited AL', 'US','UM','SUM','BM','SBM')])

# AL recruitment KPIs -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
AL_recruitment_KPIs <- list(
  result_index,
  dplyr::select(dplyr::filter(dplyr::ungroup(final_leader), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_active_leader), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_recruite_leader), TERRITORY=='NORTH'), -c(TERRITORY))
) %>% 
  Reduce(function(df1, df2) merge(x=df1, y=df2, by.x='BUSSINESSDATE_FM1', by.y='BUSSINESSDATE', all.x=TRUE), .) %>% 
  dplyr::select(-id) %>% 
  select(-c(BUSSINESSDATE)) %>% 
  dplyr::mutate(`%active leader`=`# active leader`/`# leader`-1) %>% 
  dplyr::mutate(`avg recruit/ a. leader`=`# recruit`/`# leader`)


# APE ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Change rows' name of final_Manpower_MDRT so that it can be places the same row with final_Manpower_GenLion
names(final_APE_MDRT) <- gsub('MDRT', 'GenLion', names(final_APE_MDRT))
final_APE_GenLion <- rbind(final_APE_GenLion,final_APE_MDRT)
APE <- list(
  result_index,
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_GenLion), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_Rookie_in_month), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_Rookie_last_month), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_Rookie_2_3_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_Rookie_4_6_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_Rookie_7_12_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_Rookie_13_and_more_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_APE_SA_agents), TERRITORY=='NORTH'), -c(TERRITORY))
) %>% 
  Reduce(function(df1, df2) merge(x=df1, y=df2, by.x='BUSSINESSDATE_FM1', by.y='BUSSINESSDATE', all.x=TRUE), .) %>% 
  dplyr::select(-id) %>% 
  select(-c(BUSSINESSDATE))

APE[,'Total'] = rowSums(APE[,c('GenLion','Rookie in month','Rookie last month','2-3 months','4 - 6 mths','7-12mth','13+mth','SA')], na.rm = T)
# replase all 0 by NA
APE$`Total`[APE$`Total`==0] <- NA

# Active ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Active <- list(
  result_index,
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_MDRT), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_GenLion), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_Rookie_in_month), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_Rookie_last_month), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_Rookie_2_3_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_Rookie_4_6_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_Rookie_7_12_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_Rookie_13_and_more_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_SA_agents), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Active_total_agents), TERRITORY=='NORTH'), -c(TERRITORY))
) %>% 
  Reduce(function(df1, df2) merge(x=df1, y=df2, by.x='BUSSINESSDATE_FM1', by.y='BUSSINESSDATE', all.x=TRUE), .) %>% 
  dplyr::select(-id) %>% 
  select(-c(BUSSINESSDATE))
# Active[,'Total'] = rowSums(Active[,c('GenLion','Rookie in month','Rookie last month','2-3 months','4 - 6 mths','7-12mth','13+mth','SA')], na.rm = T)
# # replase all 0 by NA
# Active$`Total`[Active$`Total`==0] <- NA



# Manpower Genlion Rookie -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Change rows' name of final_Manpower_MDRT so that it can be places the same row with final_Manpower_GenLion
names(final_Manpower_MDRT) <- gsub('MDRT', 'GenLion', names(final_Manpower_MDRT))
final_Manpower_GenLion <- rbind(final_Manpower_GenLion,final_Manpower_MDRT)
Manpower_Genlion_Rookie <- list(
  result_index,
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_GenLion), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_Rookie_in_month), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_Rookie_last_month), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_Rookie_2_3_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_Rookie_4_6_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_Rookie_7_12_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_Rookie_13_and_more_months), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_SA_agents), TERRITORY=='NORTH'), -c(TERRITORY)),
  dplyr::select(dplyr::filter(dplyr::ungroup(final_Manpower_total_agents), TERRITORY=='NORTH'), -c(TERRITORY))
) %>% 
  Reduce(function(df1, df2) merge(x=df1, y=df2, by.x='BUSSINESSDATE_FM1', by.y='BUSSINESSDATE', all.x=TRUE), .) %>% 
  dplyr::select(-id) %>% 
  select(-c(BUSSINESSDATE))

# *** ---------------------------------------------------------------------
# add prefix to colnames --------------------------------------------------
colnames(North_Overall_KPI_performance) <- paste('Overall KPI performance', colnames(North_Overall_KPI_performance), sep=':')
colnames(North_Ending_MP) <- paste('Ending MP', colnames(North_Ending_MP), sep=':')
colnames(Recruitment) <- paste('Recruitment', colnames(Recruitment), sep=':')
colnames(APE) <- paste('APE', colnames(APE), sep=':')
colnames(Active) <- paste('# Active', colnames(Active), sep=':')
colnames(AL_recruitment_KPIs) <- paste('AL recruitment KPIs', colnames(AL_recruitment_KPIs), sep=':')
colnames(Manpower_Genlion_Rookie) <- paste('Manpower_Genlion_Rookie', colnames(Manpower_Genlion_Rookie), sep=':')

# *** ---------------------------------------------------------------------
# merge all data tables to one --------------------------------------------
North <- North_Overall_KPI_performance %>% 
  merge(x=., y=North_Ending_MP,
        by.x = c('Overall KPI performance:BUSSINESSDATE_FM1'),
        by.y = c('Ending MP:BUSSINESSDATE_FM1' ),
        all.x = TRUE) %>%
  merge(x=., y=Recruitment,
        by.x = c('Overall KPI performance:BUSSINESSDATE_FM1'),
        by.y = c('Recruitment:BUSSINESSDATE_FM1' ),
        all.x = TRUE) %>%
  merge(x=., y=AL_recruitment_KPIs,
        by.x = c('Overall KPI performance:BUSSINESSDATE_FM1'),
        by.y = c('AL recruitment KPIs:BUSSINESSDATE_FM1' ),
        all.x = TRUE) %>%
  merge(x=., y=APE,
        by.x = c('Overall KPI performance:BUSSINESSDATE_FM1'),
        by.y = c('APE:BUSSINESSDATE_FM1' ),
        all.x = TRUE) %>%
  merge(x=., y=Active,
        by.x = c('Overall KPI performance:BUSSINESSDATE_FM1'),
        by.y = c('# Active:BUSSINESSDATE_FM1' ),
        all.x = TRUE) %>%
  merge(x=., y=Manpower_Genlion_Rookie,
        by.x = c('Overall KPI performance:BUSSINESSDATE_FM1'),
        by.y = c('Manpower_Genlion_Rookie:BUSSINESSDATE_FM1' ),
        all.x = TRUE) %>%
  dplyr::arrange(`Overall KPI performance:id`) %>% # sort by id
  dplyr::select(-`Overall KPI performance:id`) 

# *** ---------------------------------------------------------------------
# Transpose data table: rows to columns -----------------------------------
# North_results <- setNames(data.frame(t(North_results[,-1]), stringsAsFactors = F), North_results[,1])
North_results <- North %>% 
  select(-c(`Overall KPI performance:BUSSINESSDATE_FM1`,`Overall KPI performance:BUSSINESSDATE`))%>% # remove unnecessary columns so that all the cells' datatype will be kept
  t() %>% 
  data.frame(stringsAsFactors = F) %>% 
  # set columns' name by the first row's values
  setNames(North[,1]) 

# *** ---------------------------------------------------------------------
# Remove columns from dataframe where ALL values are NA -------------------
#excluded all columns whose total na values of a column = total row ####
# North_results <- North_results[,colSums(is.na(North_results))<nrow(North_results)] 
North_results <- dplyr::filter(North_results, colSums(is.na(North_results))<nrow(North_results))


# *** ---------------------------------------------------------------------
# Create compare YoY ------------------------------------------------------
# North_results <- compare_yoy(
#   North_results,
#   head(grep("^YoY[0-9]{2}$", names(North_results)),1),
#   tail(grep("^YoY[0-9]{2}$", names(North_results)),1), 
#   T) 

# *** ---------------------------------------------------------------------
# Create compare MoM ------------------------------------------------------
# North_results <- compare_mom_qoq(
#   North_results,
#   head(grep("^MoM.*[0-9]{2}$", names(North_results)),1),
#   tail(grep("^MoM.*[0-9]{2}$", names(North_results)),1),
#   T) 

# *** ---------------------------------------------------------------------
# add separated columns afther the last columns whose prefix is YoY -------
# colidx = tail(grep("^YoY", names(North_results)),1)
# North_results <- add_separated_col(North_results, colidx, sprintf('sep%s', colidx))
# colidx = tail(grep("^Q.*[0-9]{2}", names(North_results)),1)
# North_results <- add_separated_col(North_results, colidx, sprintf('sep%s', colidx))
# colidx = head(grep("^MoM", names(North_results)),1)-1
# North_results <- add_separated_col(North_results, colidx, sprintf('sep%s', colidx))

# add separated headers ####
# North_results <- rbind(
#   North_results[1:(head(grep("^Ending MP:", rownames(North_results)),1)-1),],
#   colnames(North_results),
#   North_results[head(grep("^Ending MP:", rownames(North_results)),1):nrow(North_results),]
# )