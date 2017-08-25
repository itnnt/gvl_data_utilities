library(RDCOMClient)
rowNameColIndex = 1
headerRowIndex = 1
xlApp <- COMCreate("Excel.Application")
xlApp$Quit()               # close running Excel

excelfile="D:\\DOM\\DIEM\\SP 18-22\\1.0. GVL Agency Sales Plan_2018 2022_working file (July 2017)_Tung_updated.xlsx"
wb    <- xlApp[["Workbooks"]]$Open(excelfile)

sheet <- wb$Worksheets("Agency North")


# Make Excel workbook visible to user
xlApp[["Visible"]] <- FALSE

# change the value of a single cell
cell  <- sheet$Cells(11,12)
cell[["Value"]] <- 3.2322323

# change the value of a range
range <- sheet$Range("A1:F1")
range[["Value"]] <- paste("Col",1:6,sep="-")

# select range
sheet$Range("A1:Z1")$Select()
wb$ActiveSheet()$Range("A1:AA4")$Select()

# Force to Overwrite An Excel File through RDCOMClient Package in R, 
# Ghi đè file sẵn có không cần thông báo
xlApp[["DisplayAlerts"]] <- FALSE

wb$Save()                  # save the workbook
wb$SaveAS("new.file.xls")  # save as a new workbook
xlApp$Quit()               # close Excel



# test --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

r = sheet$UsedRange()
print(r[["Columns"]]$Count() )
print(r[["Rows"]]$Count() )

# Get the data from the 3rd column.
r = sheet$Range(sheet$Cells(1,3), sheet$Cells(r[["Rows"]]$Count(), 3))
data = unlist(r[["Value"]])

# Get the data from the 1st column.
r = sheet$Range(sheet$Cells(1,1), sheet$Cells(r[["Rows"]]$Count(), 1))
data = unlist(r[["Value"]])

# Get the data from the 1st row
r = sheet$Range(sheet$Cells(1,1), sheet$Cells(1, r[["Columns"]]$Count()))
data = unlist(r[["Value"]])

range[["EntireRow"]]

RDCOMClient_get_row_values(wb, "Agency North", 1)
RDCOMClient_row_count(wb, "Agency North")
unlist(r$Rows()$Item(97)$Value())  

RDCOMClient_get_col_values(wb, "Agency North", 1)
RDCOMClient_col_count(wb, "Agency North")
unlist(r$Columns()$Item(1)$Value()) 

val = sheet$Cells(1,122)$Value()

r$Columns()$Item(1)$Value()[[1]][[1]]


tem=(r$Columns()$Item(1)$Value())
unlist(r[['Columns']]$Item(1)$Value())  

c1 <- r$Column()              ## first col
c2 <- r$Columns()$Count()        ## last col, provided contiguous region
r1 <- r$Row()                    ## first row
r2 <- r$Rows()$Count()           ## last row, provided contiguous region
# test --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------





## TODO: 
##   row.names for importing/exporting data.frames
##   dates/currency, general converters
##   error checking

"exportDataFrame" <-
  function(df, at, ...)
    ## export the data.frame "df" into the location "at" (top,left cell)
    ## output the occupying range.
    ## TODO: row.names, more error checking
  {
    nc <- dim(df)[2]
    if(nc<1) stop("data.frame must have at least one column")
    r1 <- at$Row()                   ## 1st row in range
    c1 <- at$Column()                ## 1st col in range
    c2 <- c1 + nc - 1                ## last col (*not* num of col)
    ws <- at[["Worksheet"]]
    
    ## headers
    
    hdrRng <- ws$Range(ws$Cells(r1, c1), ws$Cells(r1, c2))
    hdrRng[["Value"]] <- names(df)
    
    ## data
    
    rng <- ws$Cells(r1+1, c1)        ## top cell to put 1st column 
    for(j in seq(from = 1, to = nc)){
      cat("Column", j, "\n")
      exportVector(df[,j], at = rng, ...)
      rng <- rng$Next()            ## next cell to the right
    }
    invisible(ws$Range(ws$Cells(r1, c1), ws$Cells(r1 + nrow(df), c2)))
  }


"importDataFrame" <-
  function(rng = NULL, wks = NULL, n.guess = 5, dateFun = as.chron.excelDate)
    ## Create a data.frame from the range rng or from the "Used range" in
    ## the worksheet wks.  The excel data is assumed to be a "database" (sic) 
    ## excel of primitive type (and possibly time/dates).
    ## We guess at the type of each "column" by looking at the first
    ## n.guess entries ... but it is only a very rough guess.
  {
    if(is.null(rng) && is.null(wks))
      stop("need to specify either a range or a worksheet")
    if(is.null(rng))
      rng <- wks$UsedRange()          ## actual region
    else
      wks <- rng[["Worksheet"]]       ## need to query rng for its class
    n.areas <- rng$Areas()$Count()     ## must have only one region
    if(n.areas!=1)
      stop("data must be in a contigious block of cells")
    
    c1 <- rng$Column()                 ## first col
    c2 <- rng$Columns()$Count()        ## last col, provided contiguous region
    r1 <- rng$Row()                    ## first row
    r2 <- rng$Rows()$Count()           ## last row, provided contiguous region
    
    ## headers
    
    n.hdrs <- rng$ListHeaderRows()
    if(n.hdrs==0)
      hdr <- paste("V", seq(form=1, to=c2-c1+1), sep="")
    else if(n.hdrs==1) 
      hdr <- unlist(rng$Rows()$Item(r1)$Value2())   
    else {    ## collapse multi-row headers
      h <- vector("list", c2-c1+1)     ## list by column
      r <- rng$Range(rng$Cells(r1,c1), rng$Cells(r1+n.hdrs-1, c2))
      jj <- 1
      for(j in seq(from=c1, to=c2)){
        h[[jj]] <- unlist(r$Columns(j)$Value2()[[1]])
        jj <- jj+1
      }
      hdr <- sapply(h, paste, collapse=".")
    }
    r1 <- r1 + n.hdrs
    
    ## Data region 
    
    d1 <- wks$Cells(r1, c1)
    d2 <- wks$Cells(r2, c2)
    dataCols <- wks$Range(d1, d2)$Columns()
    out <- vector("list", length(hdr))
    for(j in seq(along = out)){
      f1 <- dataCols$Item(j)
      f2 <- f1$Value2()[[1]]
      f <- unlist(lapply(f2, function(x) if(is.null(x)) NA else x))
      cls <- guessExcelColType(f1)
      out[[j]] <- if(cls=="logical") as.logical(f) else f
    }
    names(out) <- make.names(hdr)
    as.data.frame(out)
  }