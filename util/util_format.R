rename_columns <- function(columns) {
  columns <- gsub("AGCODE", "AGENTCD", columns)
  columns <- gsub('AGCode', 'AGENTCD', columns)
  
  columns <- gsub('BenefitCode', 'BENEFITCODE', columns)
  
  columns <- gsub('TOO_Bonus', 'EXPECT', columns)
  columns <- gsub('TOOL_TOTALAMT', 'EXPECT', columns)
  
  columns <- gsub('EDM_Bonus', 'EDM', columns)
  columns <- gsub('EDM_TOTALAMT', 'EDM', columns)
  
  columns <- gsub('Check_Bonus', 'CHECK', columns)
  columns <- gsub('CHECK_TOTALAMT', 'CHECK', columns)
  
  columns <- gsub('BENEFITRESULTSEQ', 'SEQ', columns)
  columns <- gsub('PAYOUTSEQ', 'SEQ', columns)
  columns
}