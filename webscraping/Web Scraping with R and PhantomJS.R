# Content
# 
# Load the necessary packages
# Scraping Javascript Generated Data with R
# Conclusion
# 
# Load the necessary packages

# As a first step, you'll have to load all packages that we need for this analysis 
# into the workspace 
# (if you haven't installed these packages on your local system yet, 
# use install.packages() to make them available):
#   
# library(rvest)
# library(stringr)
# library(plyr)
# library(dplyr)
# library(ggvis)
# library(knitr)
# options(digits = 4)
# 
# Scraping Javascript Generated Data with R

# The next step is the collection of the TechStars data using PhantomJS. Check out the following basic .js file:
#   
#   // scrape_techstars.js
# 
# var webPage = require('webpage');
# var page = webPage.create();
# 
# var fs = require('fs');
# var path = 'techstars.html'
# 
# page.open('http://www.techstars.com/companies/stats/', function (status) {
#   var content = page.content;
#   fs.write(path,content,'w')
#   phantom.exit();
# });
# 
# The script basically renders the HTML page after the underlying javascript code has done its work, allowing you to fetch the HTML page, with all the tables in there. To stay in R for the rest of this analysis, we suggest you use the system() function to invoke PhantomJS (you'll have to download and install PhantomJS and put it in your working directory):

# Let phantomJS scrape techstars, output is written to techstars.html
# system("./phantomjs scrape_techstars.js")
# -------------------------------------------------
# After this small detour, you finally have an HTML file, techstars.html, on our local system, that can be scrape with rvest. An inspection of the Techstars webpage reveals that the tables we're interested in are located in divs with the css class batch:
# -------------------------------------------------
# batches <- html("techstars.html") %>%
# html_nodes(".batch")
# 
# class(batches)
# -------------------------------------------------
# [1] "XMLNodeSet"
# 
# You now have a list of XMLNodeSet objects: each object contains the data for a single TechStars batch. In there, we can find information concerning the batch location, the year, the season, but also about the companies, their current headquarters, their current status and the amount of funding they raised in total. We will not go into detail on the data collection and cleaning steps below; you can execute the code yourself and inspect what they accomplish. You'll see that some custom cleaning is going on to make sure that each bit of information is nicely formatted:
#   
#   batch_titles <- batches %>%
#     html_nodes(".batch_class") %>%
#     html_text()
#   
#   batch_season <- str_extract(batch_titles, "(Fall|Spring|Winter|Summer)")
#   batch_year <- str_extract(batch_titles, "([[:digit:]]{4})")
#   # location info is everything in the batch title that is not year info or season info
#   batch_location <- sub("\\s+$", "",
#                         sub("([[:digit:]]{4})", "",
#                             sub("(Fall|Spring|Winter|Summer)","",batch_titles)))
#   
#   # create data frame with batch info.
#   batch_info <- data.frame(location = batch_location,
#                            year = batch_year,
#                            season = batch_season)
#   
#   breakdown <- lapply(batches, function(x) {
#     company_info <- x %>% html_nodes(".parent")
#     companies_single_batch <- lapply(company_info, function(y){
#       as.list(gsub("\\[\\+\\]\\[\\-\\]\\s", "", y %>%
#                      html_nodes("td") %>%
#                      html_text()))
#     })
#     df <- data.frame(matrix(unlist(companies_single_batch),
#                             nrow=length(companies_single_batch),
#                             byrow=T,
#                             dimnames = list(NULL, c("company","funding","status","hq"))))
#     return(df)
#   })
#   
#   # Add batch info to breakdown
#   batch_info_extended <- batch_info[rep(seq_len(nrow(batch_info)),
#                                         sapply(breakdown, nrow)),]
#   breakdown_merged <- rbind.fill(breakdown)
#   
#   # Merge all information
#   techstars <- tbl_df(cbind(breakdown_merged, batch_info_extended)) %>%
#     mutate(funding = as.numeric(gsub(",","",gsub("\\$","",funding))))
#   
#   With a combination of core R, rvest, plyr and dplyr functions, we now we have the techstars data frame; a data set of all TechStars company, with all publicly available information that is nicely formatted:
#     
#     techstars
#   
#   ## Source: local data frame [535 x 7]
#   ##
#   ##          company funding   status                hq location year season
#   ## 1    Accountable  110000   Active    Fort Worth, TX   Austin 2013   Fall
#   ## 2          Atlas 1180000   Active        Austin, TX   Austin 2013   Fall
#   ## 3        Embrace  110000   Failed        Austin, TX   Austin 2013   Fall
#   ## 4  Filament Labs 1490000   Active        Austin, TX   Austin 2013   Fall
#   ## 5        Fosbury  300000   Active        Austin, TX   Austin 2013   Fall
#   ## 6          Gone!  840000   Active San Francisco, CA   Austin 2013   Fall
#   ## 7     MarketVibe  110000 Acquired        Austin, TX   Austin 2013   Fall
#   ## 8           Plum 1630000   Active        Austin, TX   Austin 2013   Fall
#   ## 9  ProtoExchange  110000   Active        Austin, TX   Austin 2013   Fall
#   ## 10       Testlio 1020000   Active        Austin, TX   Austin 2013   Fall
#   ## ..           ...     ...      ...               ...      ...  ...    ...
#   
#   names(techstars)
#   
#   ## [1] "company"  "funding"  "status"   "hq"       "location" "year"
#   ## [7] "season"
#   
#   Conclusion
#   
#   Using the above combination of tools and code, 
#   we managed to scrape data from a website that uses a JavaScript script 
#   to generate its data. As one can see, this is a very structured process, 
#   that can be easily done once the initial code is available. 
#   Want to learn R, and looking for data science and R tutorials and courses? 
#   Check out DataCamp





library(rvest)
library(stringr)
library(plyr)
library(dplyr)
library(ggvis)
library(knitr)
options(digits = 4)

# Let phantomJS scrape techstars, output is written to techstars.html
system("./webscraping/phantomjs-2.1.1-windows/bin/phantomjs ./webscraping/scrape_techstars.js")

batches <- read_html("./techstars.html") %>% html_nodes(".batch")
class(batches)

batch_titles <- batches %>%
  html_nodes(".batch_class") %>%
  html_text()

batch_season <- str_extract(batch_titles, "(Fall|Spring|Winter|Summer)")
batch_year <- str_extract(batch_titles, "([[:digit:]]{4})")

# location info is everything in the batch title that is not year info or season info
batch_location <- sub("\\s+$", "",
                      sub("([[:digit:]]{4})", "",
                          sub("(Fall|Spring|Winter|Summer)","",batch_titles)))

# create data frame with batch info.
batch_info <- data.frame(location = batch_location,
                         year = batch_year,
                         season = batch_season)

breakdown <- lapply(batches, function(x) {
  company_info <- x %>% html_nodes(".parent")
  companies_single_batch <- lapply(company_info, function(y){
    as.list(gsub("\\[\\+\\]\\[\\-\\]\\s", "", y %>%
                                              html_nodes("td") %>%
                                              html_text()
                )
            )
  })
  df <- data.frame(matrix(unlist(companies_single_batch),
                          nrow=length(companies_single_batch),
                          byrow=T,
                          dimnames = list(NULL, c("company","funding","status","hq"))))
  return(df)
})

# Add batch info to breakdown
batch_info_extended <- batch_info[rep(seq_len(nrow(batch_info)),
                                      sapply(breakdown, nrow)),]
breakdown_merged <- plyr::rbind.fill(breakdown)

# Merge all information
techstars <- tbl_df(cbind(breakdown_merged, batch_info_extended)) %>%
  mutate(funding = as.numeric(gsub(",","",gsub("\\$","",funding))))
