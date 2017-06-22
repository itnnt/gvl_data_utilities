library(readr)
income_2015<- read_csv("D:/workspace_r/gvl_data_utilities/income_statement/data/income_2015.csv")
income_2016 <- read_csv("D:/workspace_r/gvl_data_utilities/income_statement/data/income_2016.csv")
View(income_2016)

# Force R not to use exponential notation (e.g. e+10)?
options("scipen"=100, "digits"=4, )
# xem cơ cấu phân bổ thu nhập đại lý trong năm 2015
summary(income_2015$ytd_pretax_income)
quantile(income_2015$ytd_pretax_income)
# xem cơ cấu phân bổ thu nhập đại lý trong năm 2016
summary(income_2016$ytd_pretax_income)
quantile(income_2016$ytd_pretax_income)
