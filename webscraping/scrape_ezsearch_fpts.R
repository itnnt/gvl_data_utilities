library(rvest)
library(stringr)
library(plyr)
library(dplyr)
library(ggvis)
library(knitr)
options(digits = 4)

# Let phantomJS scrape techstars, output is written to techstars.html
system("./webscraping/phantomjs-2.1.1-windows/bin/phantomjs ./webscraping/scrape_ezsearch_fpts.js")

batches <- read_html("./techstars.html") %>% html_nodes(".batch")
class(batches)
