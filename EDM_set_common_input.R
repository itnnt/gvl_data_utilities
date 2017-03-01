require(dplyr)
library(readxl)
library(WriteXLS)
library(dplyr)
library(data.table)
library(readxl)
library(tidyr)
source(paste(getwd(), '', 'EDM.R', sep = '/'))
source(paste(getwd(), 'util', 'util_extract_data.R', sep = '/'))
source(paste(getwd(), 'util', 'util_format.R', sep = '/'))

# set output folder ####
output = 'S:/IT/Share to others/tung'
trim <- function (x) gsub("^\\s+|\\s+$", "", x)
gen_cps_row <- function(sample_cps, temp, kpidef, products) {
  # _create a new row for CMS_PRODUCTION_SUMMARY: PERSONAL ####
  sample_cps$PRODNUNITCD = temp$PRODNUNITCD
  sample_cps$PRODNBASISCD = dplyr::select(dplyr::filter(kpidef, PRODNUNITCD == as.character(temp$PRODNUNITCD)), PRODNUNITBASISCD)
  if (!(is.na(temp$PRODUCTCD))) {
    product = dplyr::select(dplyr::filter(products, PRODUCTCD == as.character(temp$PRODUCTCD)), PRODUCTSEQ, PRODUCTCD)
    sample_cps$PRODUCTSEQ = product$PRODUCTSEQ
    sample_cps$PRODUCTCD = product$PRODUCTCD
  } else {
    sample_cps$PRODUCTSEQ = 0
    sample_cps$PRODUCTCD = 'NULL'
  }
  if (as.character(temp$GROUPCD) %in% c ('PERSONAL', 'PERSONNEL')) {
    sample_cps$PRODNGROUPCD = 'PERSONNEL'
    sample_cps$PRODNTYPECD = 'PERSONNEL'
  } else {
    sample_cps$PRODNGROUPCD = temp$GROUPCD
    sample_cps$PRODNTYPECD = 'GROUP'
  }
  sample_cps$PRODNVALUE = temp$TOOL
  sample_cps$PRODNVALUE <- format(sample_cps$PRODNVALUE, scientific=FALSE)
  sample_cps$CREATEDBY = 'TUNGNGUYEN'
  sample_cps$UPDATEDBY = 'TUNGNGUYEN'
  scp = paste('@NEWSEQ', sample_cps$PRODNUNITCD, agentcd, sample_cps$PRODNGROUPCD, sample_cps$PRODUCTCD, sep='_')
  sample_cps$SCOPE_IDENTITY=paste("DECLARE", scp, 'NUMERIC(30,0)=SCOPE_IDENTITY();', sep=' ')
  # save the new row to output file csv
  sample_cps
}