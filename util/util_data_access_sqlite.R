# Load the SQLite library
library("RSQLite")

sqlite_retrieve <- function (sql, databasefile) {
  # Assign the sqlite datbase and full path to a variable
  dbfile = databasefile;
  # Instantiate the dbDriver to a convenient object
  sqlite = dbDriver("SQLite")
  # Assign the connection string to a connection object
  dbconnection = dbConnect(sqlite, dbfile, "utf-8");
  # Request a list of tables using the connection object
  dbListTables(dbconnection)
  # Assign the results of a SQL query to an object
  results = dbSendQuery(dbconnection, sql)
  # Return results from a custom object to a data.frame
  data = fetch(results, encoding="utf-8")
  # Encoding(data$DATATYPE) <- "UTF-8"
  # format numeric columns
  # data$Q4<-prettyNum(data$Q4,big.mark=",",scientific=FALSE)
  # Clear the results and close the connection
  dbClearResult(results)
  dbDisconnect(dbconnection)
  # return data
  data
}

get_agent_income <- function (y, m, h, agentcd, databasefile) {
  sql = sprintf(
    'SELECT * FROM summary_income_4JasperPrinting WHERE income_year=%s AND income_month=%s AND incomeMidmonth=%s',
    y,
    m,
    h
  )
  sql <- paste(sql, sprintf("agent_code='%s'", agentcd), sep = ' AND ')
  sqlite_retrieve(sql, databasefile)
}