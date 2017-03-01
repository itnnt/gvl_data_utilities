library(readxl)

dfuid = "DFUWF-943"
extracted_filename = "INHOUSE_BANKSTAFF_PRODUCTION_DATA_EDM_UAT6_2017-05-29.xlsx"
user_checked_file = "DFU_CheckDay_20170529_1.xlsx"

group <- read_excel(sprintf("S:/IT/Share to others/tung/%s/%s", dfuid, extracted_filename), sheet = "Group")
Personal <- read_excel(sprintf("S:/IT/Share to others/tung/%s/%s", dfuid, extracted_filename), sheet = "Personal")

group_user <- read_excel(sprintf("S:/IT/Share to others/tung/%s/%s", dfuid, user_checked_file), sheet = "KPI_SUM")
fieldnames <- names(group_user)
fieldnames <- gsub('AGCode', 'AGENTCD', fieldnames)
fieldnames <- gsub('RDOCNUM', 'POLICYCD', fieldnames)
fieldnames <- gsub('AGCODE', 'AGENTCD', fieldnames)
fieldnames <- gsub('LEADER', 'LEADERCD', fieldnames)
fieldnames <- gsub('LEADERCDCD', 'LEADERCD', fieldnames)
fieldnames <- gsub('\\<REPORTINGCD\\>', 'LEADERCD', fieldnames)
fieldnames <- gsub('KPICODE', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<KPI_\\>', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<KPI\\>', 'PRODNUNITCD', fieldnames)
fieldnames <- gsub('\\<GVL\\>', 'TOOL', fieldnames)
fieldnames <- gsub('\\<IT\\>', 'EDM', fieldnames)
fieldnames <- gsub('ProductCode', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('productcd', 'PRODUCTCD', fieldnames)
fieldnames <- gsub('\\<GROUP\\>', 'GROUPCD', fieldnames)
fieldnames <- gsub('Business date', 'BUSINESSDT', fieldnames)
names(group_user) <- fieldnames

# left join
group_merge <- merge(
  x = group_user,
  y = dplyr::select(group,  c(PRODUCERSOURCECD, PRODNUNITCD, PRODUCTCD, PRODNGROUPCD, PRODNSUMMSEQ, PRODNVALUE)),
  by.x = c("AGENTCD", "PRODNUNITCD", "PRODUCTCD", "GROUPCD" ),
  by.y = c("PRODUCERSOURCECD", 'PRODNUNITCD', 'PRODUCTCD', 'PRODNGROUPCD' ),
  all.x = TRUE
)

personal_merge <- merge(
  x = group_user,
  y = dplyr::select(Personal,  c(producersourcecd, PRODNUNITCD, PRODUCTCD, PRODNTYPECD, PRODNSUMMSEQ, PRODNVALUE)),
  by.x = c("AGENTCD", "PRODNUNITCD", "PRODUCTCD", "GROUPCD" ),
  by.y = c("producersourcecd", 'PRODNUNITCD', 'PRODUCTCD', 'PRODNTYPECD' ),
  all.x = TRUE
)

group_merge <- dplyr::filter(group_merge, !is.na(PRODNSUMMSEQ))
personal_merge <- dplyr::filter(personal_merge, !is.na(PRODNSUMMSEQ))

group_merge$SQL <- sprintf("UPDATE CMS_PRODUCTION_SUMMARY SET PRODNVALUE=%s WHERE PRODNSUMMSEQ=%s", group_merge$TOOL, group_merge$PRODNSUMMSEQ)
personal_merge$SQL <- sprintf("UPDATE CMS_PRODUCTION_SUMMARY SET PRODNVALUE=%s WHERE PRODNSUMMSEQ=%s", personal_merge$TOOL, personal_merge$PRODNSUMMSEQ)

# writeline ---------------------------------------------------------------
writeLines(group_merge$SQL, sprintf("S:/IT/Share to others/tung/%s/output_script1.sql", dfuid), useBytes=T)
writeLines(personal_merge$SQL, sprintf("S:/IT/Share to others/tung/%s/output_script2.sql", dfuid), useBytes=T)

