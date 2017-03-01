source('EDM_set_common_input.R')
env = "COMPANY_EDM_VN"
sql<-"
SELECT CPM.PRODUCERSOURCECD, ADJUSTMENTAMT, YEAR(DUEDT)-1 AS PITYR FROM CMS_PAYMENT_ADJUSTMENT CPA 
JOIN dbo.CMS_PRODUCERCHANNEL_M CPM ON CPM.PRODUCERCD = CPA.PRODUCERCD
WHERE DUEDT='2017-01-31' AND REMARKS LIKE 'PIT%'
"

pit <- execute_sql(sql, env)
pit$update <- sprintf("UPDATE SUMMARY_AGENT_TAX SET TOTAL_TAX=TOTAL_TAX+%s WHERE AGENTCD='%s' AND YEAR(CALFROM)=%s", pit$ADJUSTMENTAMT, pit$PRODUCERSOURCECD, pit$PITYR)
save_to_sql_file('', 'filename.sql', append = FALSE)
save_to_sql_file(pit$update, 'filename.sql', append = TRUE)

pit$update1 <- sprintf("UPDATE summary_income_4JasperPrinting SET ytd_tax=ytd_tax+%s WHERE agent_code='%s' AND income_year=%s and income_month=12 and incomeMidmonth=2", pit$ADJUSTMENTAMT, pit$PRODUCERSOURCECD, pit$PITYR)
save_to_sql_file('', 'filename1.sql', append = FALSE)
save_to_sql_file(pit$update1, 'filename1.sql', append = TRUE)