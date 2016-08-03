
#################
# Change to FALSE before releasing
# This will restrict the application to using smaller eval datasets only = quicker testing
#TESTING <- TRUE
#################

# Load configuration parameters to a data table
# Currently a ':' delimited file as only one entry
application_config <<- fread('config.txt', sep="|", header=FALSE)
setnames(application_config, names(application_config), c("Key", "Value"))

# Initialise Mongo connection
# mongolimit is max number of records to fetch from mongo
# This greatly affects performance of fetches on data tabs. 15000 = 2.5s; 5000 = 0.7s;
mongo_limit <- application_config[which(application_config$Key == 'mongolimit'),]$Value
mongohost <- application_config[which(application_config$Key == 'mongohost'),]$Value
mongo = mongo.create(host=mongohost)
mongo.is.connected(mongo)
mongo.get.databases(mongo)

TESTING <- application_config[which(application_config$Key == 'testing'),]$Value == 'TRUE'

if (TESTING) {
  testcollections <- application_config[which(application_config$Key == 'testcollections'),]$Value
  testcollections <- unlist(strsplit(testcollections, ";"))
}

datafolder <- application_config[which(application_config$Key == 'datafolder'),]$Value

# Initialise variables
A_file <<- data.table()   # container for original data file A
B_file <<- data.table()   # container for original data file B
links <<- data.table()    # container for links between A and B

# List of columns shown on details tabs. Updated later by function:update_data_files
# Changing these means changing references to them throughout the code, so DO NOT TOUCH
display_columns_start <<- c("series_A", "IAID_A", "series_B", "IAID_B","Score", "Confidence") 
display_columns <<- display_columns_start
source_tsv_column_names <- c("Forenames","Surnames","DOB","ServNum","BirthPlace")  # Column names which appear in the source tsv files must match this list.

# These variables should be renamed
m_results <<- data.frame()
current_results <<- data.frame()
original_mongo_data <<- list()
schema_name <<- "ttt."
logging_table <<- paste0(schema_name, "logging")

series_lookup <- data.frame(IAID = c("C16062", "C15126", "C14284", "C14543", "C2130", "C15486", "C1981"),
                            name = c("ADM340", "ADM337", "WO76",   "WO339",  "AIR76", "ADM339", "ADM273"))

series_lookup <- new.env()
key <- "C16062"; value <- "ADM340"
assign(key, value, series_lookup, inherits=FALSE)
assign("C15126", "ADM337", series_lookup, inherits=FALSE)
assign("C14284", "WO76", series_lookup, inherits=FALSE)
assign("C14543", "WO339", series_lookup, inherits=FALSE)
assign("C2130", "AIR76", series_lookup, inherits=FALSE)
assign("C15486", "ADM339", series_lookup, inherits=FALSE)
assign("C1981", "ADM273", series_lookup, inherits=FALSE)

get_series <- function(iaid) {
  series_name <- series_lookup[[iaid]]
  if (is.null(series_name)) {
    return(iaid)
  } else {
    return(series_name)
  }
}
get_series("C16062")
get_series("C16063")

# Function to trim leading zeros from dates
rem_lead0 <- function(date) {
  out_val <- date
  if (!is.na(date) > 0 && substr(date, 1, 1) == 0) {
     out_val <- substr(date, 2, nchar(date))
  }
  out_val
}

confidenceCurve <- function(score, slope, centrePoint) {
  return (round(1 / ((1 + (exp(-slope * (score - centrePoint))))) * 100, 2))
}


# Create a link to Discovery details page
# Used for viewing original documents
gendiscolink <- function(iaid) {
  sprintf("<a href='http://discovery.nationalarchives.gov.uk/details/r/%s' target='_blank'>%s</a>",iaid,iaid)
}

genseriesname <- function(TTTid) {
  #series_id <- strsplit(strsplit(TTTid, "-")[[1]][2], "_")[[1]][1]
  series_id <- get_series(strsplit(strsplit(TTTid, "-")[[1]][2], "_")[[1]][1])
  return(series_id)
}


update_data_files <- function(a_file, b_file) {
  # Read the first file into container initialised above. Expects file to be tab delimited with a header row.
  source_file_name_A <- paste0(datafolder, a_file, ".tsv")
  if (file.exists(source_file_name_A)) {
    A_file <<- fread(source_file_name_A, sep="\t", header=T)
    display_columns <<- display_columns_start
    # Update header row to include indicator that this is source file A
    setnames(A_file, c("TTTid", "IAID"), c("TTTid_A", "source_IAID_A"))
    # Remove leading zeroes from the date of birth column if it is in the file
    if ("DOB" %in% names(A_file)) {
      A_file$DOB <<- mapply(rem_lead0, A_file$DOB)
    }
    # Add A to the column names so we know which source they refer to
    for (col in source_tsv_column_names) {
      if (col %in% names(A_file) & !(col %in% display_columns)) {
        new_col <- paste0(col, "_A")
        setnames(A_file, c(col), c(new_col))
        display_columns <<- append(display_columns, new_col)
      }
    }
    source_file_A_exists <<- TRUE
  } else {
    source_file_A_exists <<- FALSE
  }
  
  # Repeat the above steps for source file B - R is pass by value so creating a function to do both isn't easiest option
  source_file_name_B <- paste0(datafolder, b_file, ".tsv")
  if (file.exists(source_file_name_B)) {
      B_file <<- fread(source_file_name_B, sep="\t", header=T)
    setnames(B_file, c("TTTid", "IAID"), c("TTTid_B", "source_IAID_B"))
    if ("DOB" %in% names(B_file)) {
      B_file$DOB <<- mapply(rem_lead0, B_file$DOB)
    }
    for (col in source_tsv_column_names) {
      if (col %in% names(B_file) & !paste0(col, "_B") %in% display_columns) {
        new_col <- paste0(col, "_B")
        setnames(B_file, c(col), c(new_col))
        display_columns <<- append(display_columns, new_col)
      }
    } 
    source_file_B_exists <<- TRUE
  } else {
    source_file_B_exists <<- FALSE
  }
  display_columns_final <<- display_columns

}


# Used to output individual attribute scores to a string
# Used on record detail tabs to show score breakdown for selected records
printscores <- function(rows_selected) {
  cat('\nSelected row scores:\n')
  headers = TRUE
  for (i in rows_selected) {
    # Add header row to the output, assuming there are rows to output
    if (headers) {
      cat(paste(c("Row# ", "Rec Id", "Forename","Surname ","DOB   ","SNum  "), collapse="\t"))
      cat("\n")
      headers = FALSE
    }
    #print(as.numeric(original_mongo_data[[current_results[i,c("row_id")]]]$scoreDetails$scoreOnServiceReference))
    forename_score <- as.numeric(original_mongo_data[[current_results[i,c("row_id")]]]$scoreDetails$scoreOnForeNames)
    surname_score <- as.numeric(original_mongo_data[[current_results[i,c("row_id")]]]$scoreDetails$scoreOnFamilyNames)
    dob_score <- as.numeric(original_mongo_data[[current_results[i,c("row_id")]]]$scoreDetails$scoreOnDateOfBirth)
    if (length(dob_score) == 0) {
      dob_score <- 0
    }
    snum_score <- as.numeric(original_mongo_data[[current_results[i,c("row_id")]]]$scoreDetails$scoreOnServiceReference)
    if (length(snum_score) == 0) {
      snum_score <- 0
    }

    score_text <- sprintf("%d\t%s\t%2.6f\t%2.6f\t%2.4f\t%2.4f\n",
                          i,
                          row.names(current_results[i,]),
                          forename_score,
                          surname_score,
                          dob_score,
                          snum_score)
    cat(score_text)
  }
  
}


# This is the main function for reading linked records from Mongo
readlinks <- function(dataset, minscore, maxscore) {
  DBNS = dataset
  # Construct the 'where clause' for Mongo query. This will select at most 10000 records with scores in min-max range
  q_score <- sprintf('{"score":{"$gte": %.2f, "$lte": %.2f}}', minscore, maxscore)
  tmp <- mongo.find(mongo, ns=DBNS, query=q_score, limit=mongo_limit)
  
  # Convert the mongo cursor to a list that R can work with
  original_mongo_data <<- mongo.cursor.to.list(tmp)
  mongo_rows <- length(original_mongo_data)

  # If no data is returned, generate an empty data frame
  if (mongo_rows == 0) {
    mongo_results <- data.frame(TTTid_A=character(), TTTid_B=character(),
                     linkrefA=character(), linkrefB=character(), Score=numeric(), Confidence=numeric(),
                     forenameScore=numeric(), surnameScore=numeric(), DOBScore=numeric(),
                     snumScore=numeric(), row_id=numeric())
  } else {
    # This command unpacks json fields.
    # Single '[' means unpack single value attributes
    # Double '[[' means unpack attributes with sub-attributes
    # Final parameter of sapply is column range
    mongo_results <- data.frame(matrix(unlist(t(rbind(sapply(original_mongo_data,"[",2:4),
                                           sapply(original_mongo_data, "[", 6),
                                           sapply(original_mongo_data, "[[", 5)))), nrow=mongo_rows, byrow=F), stringsAsFactors=FALSE)
    # Create row_id column for later use
    mongo_results$row_id <- as.numeric(row.names(mongo_results))
  }
  print("Reading links")
  #print(names(mongo_results))
  #print(head(mongo_results,1))
  
  setnames(mongo_results, c("X1","X2","X3","X4"), c("TTTid_A","TTTid_B","Score", "Confidence"))
  
  # Update Score and Confidence columns to 2dp floats
  mongo_results <- transform(mongo_results, Score = round(as.numeric(Score),2))
  mongo_results <- transform(mongo_results, Confidence = round(as.numeric(Confidence),2))
  mongo_results
}



# Count records in mongo using a score filter
filterlinks <- function(dataset, minscore, maxscore) {
  q_score <- sprintf('{"score":{"$gte": %.2f, "$lte": %.2f}}', minscore, maxscore)
  mongo.count(mongo, ns=dataset, query=q_score)
}

# This is used to populate the log summary on the first tab
# When it is refreshed the data files are also updated.
# It is called whenever the collection drop down is updated.
readlog <- function(f) {
  print(paste("Readlog:",f))
  DBNS <- logging_table
  link_coll <- substr(f, 5, 38)  # Assuming that collection names are a fixed format
  coll_query <- sprintf('{"linker.linkerCollectionName":"%s"}', link_coll)
  tmp <- mongo.find(mongo, ns=DBNS, query=coll_query)
  data <- mongo.cursor.to.list(tmp)
  
  if (f != schema_name) {  # This just gets rid of a subscript out of bounds error before Shiny has retrieved any data
    # Update source tsv files for displaying records
    if (!is.null(data[[1]]$source$name)) {
      update_data_files(data[[1]]$source$name, data[[1]]$target$name)
    }    
  }

  data
}

# The log data is used to populate the collections drop down
read_all_log_data <- function() {
   mongo.cursor.to.data.frame(mongo.find(mongo, ns=logging_table))
}

all_log_data <<- read_all_log_data()

# Transform log entries to a simple data frame summary
parselog <- function(logdata) {
  print("Calling parselog")
  # Empty data frame to put log entries into
  df <- data.frame(Parameter=character(), Value=character())
  if (length(logdata) == 0) {
    df <- data.frame(Result="No logging data found")
  } else {
    # For each item:
    ## Find it in the log table
    collection <- logdata[[1]]$linker$linkerCollectionName
    ## Add it to the summary table
    df <- rbind(df, data.frame(Parameter="Link Source", Value=collection))

    dataA <- logdata[[1]]$source$name
    df <- rbind(df, data.frame(Parameter="Source A", Value=dataA))

    countA <- as.character(logdata[[1]]$source$count)
    # This value isn't currently in all logs. If clause will be removeable in the future, next time the logs are cleared down.
    if (length(countA) > 0) {
      df <- rbind(df, data.frame(Parameter="Records in A", Value=countA))
    }
    
    dataB <- logdata[[1]]$target$name
    df <- rbind(df, data.frame(Parameter="Source B", Value=dataB))

        countB <- as.character(logdata[[1]]$target$count)
    if (length(countB) > 0) {
      df <- rbind(df, data.frame(Parameter="Records in B", Value=countB))
    }

    run_time <- logdata[[1]]$timestamp
    run_time <- as.character.Date(run_time)
    df <- rbind(df, data.frame(Parameter="Run Time", Value=run_time))

    # opt_alg_details <- logdata[[1]]$optimizer$optimiserConfiguration
    # opt <- paste(" ",opt_alg_details$indexing, " ", sep="")
    # opt_partitioner <- opt_alg_details$partitioningAndCandidateGeneratorCombinations
    # if (length(opt_partitioner) < 2) {
    #   opt_alg <- opt_partitioner[[1]]$partitioningMethod
    # } else {
    #   opt_alg <- paste(opt_partitioner[[1]]$partitioningMethod,
    #                    opt_partitioner[[2]]$partitioningMethod, sep = opt)
    # }
    # df <- rbind(df, data.frame(Parameter="Optimiser", Value=opt_alg))
    # 
    # for (name in names(logdata[[1]]$linker$linkerConfiguration)) {
    #   df <- rbind(df, data.frame(Parameter="Linker", Value=name))
    # }
  }

  df
}



# Define server logic required to draw a histogram
server <- function(input, output, session) {
  
  # Expression that generates a histogram. The expression is
  # wrapped in a call to renderPlot to indicate that:
  #
  #  1) It is "reactive" and therefore should be automatically
  #     re-executed when inputs change
  #  2) Its output type is a plot
  
#  output$filelist <- renderText({
#    paste("Source file:", input$select)
#  })
  
  # Called by data tabs to update log and source files
  logging <- reactive({
    parselog(readlog(paste0(schema_name, input$select)))
  })
  
  # Count records in a mongo collection
  totallinks <- reactive({
    if (length(input$select) == 0) {
      return(-1)
    }
    mongo.count(mongo, ns=paste0(schema_name, input$select))
  })
  
  update_confidence <- reactive({
    print("Updating confidence")
    minscore <- input$range[1]  # Makes this component reactive to changes of score
    maxscore <- input$range[2]
    m_results$Confidence <<- sapply(m_results$Score, FUN=confidenceCurve, slope = input$slope, centrePoint = input$centrepoint)
  })
  
  
  # Creates the output data by merging links to source files
  data <- reactive({
    print("Called data")
    links <<- readlinks(paste0(schema_name,input$select), input$range[1], input$range[2])
    m_results <<- merge(x=merge(x=links,y=B_file,by="TTTid_B",all.y=FALSE),
                       y=A_file, by="TTTid_A",all.y=FALSE)
    m_results$IAID_A <<- gendiscolink(m_results$source_IAID_A)
    m_results$IAID_B <<- gendiscolink(m_results$source_IAID_B)
    if (input$show_series) {
        m_results$series_A <<- sapply(m_results$TTTid_A, FUN = genseriesname)
        m_results$series_B <<- sapply(m_results$TTTid_B, FUN = genseriesname)
        display_columns <<- display_columns_final
    } else {
      display_columns <<- display_columns_final[display_columns_final != "series_A" & display_columns_final != "series_B"]
    }
  })
  
  # Generates a summary plot by score
  # It is very slow for large datasets so perhaps could be re-written
  sumplot <- reactive({
    pipe_1 <- mongo.bson.from.JSON('{"$project":{"_id":0, "s": "$score", "y": {
      "$subtract":[
        "$score",
        {"$mod":["$score", 1]}
        ]}}}')
    pipe_3 <- mongo.bson.from.JSON('{"$group" : {"_id":"$y", "count":{"$sum":1}}}')
    #{"$match" : {"score":{$ne:NaN}}}
    #pipe_2 <- mongo.bson.from.JSON('{"$match" : {"$s" : {"$ge":0.0}}}')
    cmd_list <- list(pipe_1, pipe_3)
    if (length(input$select) == 0 || nchar(input$select) == 0) {
      return()
    } 

    res <- mongo.aggregation(mongo, paste0(schema_name,input$select), cmd_list)
    vals <- mongo.bson.value(res,"result")
    df <- data.frame(matrix(unlist(vals),nrow=length(vals), byrow=T))
    #print(nrow(df))
    if (nrow(df) == 0) {
      return()
    }
    names(df) <- c("X1","X2")
    df <- subset(df, X1 != "NaN")
    ggplot(df, aes(x=X1,y=X2)) + geom_bar(stat="identity")
    #pipe_1 <- mongo.bson.from.JSON('{"$group" : {"_id"0,:"y:$multiply:[$score,100]}", "count":{"$sum":1}}}')
    #db.contest.aggregate([
    #  {"$group" : {_id:"$province", count:{$sum:1}}}
    #  ])
  })
  


  # Shows total link count and number of links between score range
  # Used on front tab
  counts <- reactive({
    print("Calculating counts")
    totalcount <- totallinks()
    filteredcount <- filterlinks(paste0(schema_name, input$select), input$range[1], input$range[2])
    sprintf("Selected %d out of %d records", filteredcount, totalcount)
  })
  
  # This outputs the logging summary on the front tab
  output$audittrail <- renderTable({
    links <- input$select
    audit <- logging()
    audit
  })
  
  # Outputs the score histogram on the graph tab
  output$summary <- renderPlot({
    x <- input$select
    sumplot()
  })
  
  # Outputs the counts shown on the front tab
  output$link_count <- renderText({
    x <- input$select
    counts()
  })
  
  # Creates and populates the collections drop down
  output$select_result <- renderUI({
    # Populated by filters but I don't think it has been implemented correctly
    # They should union together - on second thoughts maybe this is right.
    # The filters aren't really used so will revisit when/if they become useful
    log_data <- all_log_data
    if (TESTING) {
        log_data <- subset(log_data, substr(source.name,1 ,6) %in% testcollections &
                                     substr(target.name,1 ,6) %in% testcollections)
    }
    #list("Soundex" = 1, "Metaphone" = 2, "NYSIIS" = 3, "Jaro-Winkler" = 4)
    if ("1" %in% input$logPartitionGroup) {
      log_data <- subset(log_data, optimizer.algorithm.distanceMeasure.partition == "soundex" ||
                                   optimizer.algorithm.distanceMeasure.partition.1 == "soundex")
    }
    if ("2" %in% input$logPartitionGroup) {
      log_data <- subset(log_data, optimizer.algorithm.distanceMeasure.partition == "metaphone" ||
                                   optimizer.algorithm.distanceMeasure.partition.1 == "metaphone")
    } 
    if ("3" %in% input$logPartitionGroup) {
      log_data <- subset(log_data, optimizer.algorithm.distanceMeasure.partition == "nysiis" ||
                                   optimizer.algorithm.distanceMeasure.partition.1 == "nysiis")
    }
    #list("Name" = 1, "DOB" = 2, "Service Number" = 3)
    # May be more useful to have logical operators: AND OR NOT
    if ("1" %in% input$logLinkerGroup) {
      log_data <- subset(log_data, !is.na(linker.linkerConfiguration.nameComparator.methodName))
    }
    if ("2" %in% input$logLinkerGroup) {
      log_data <- subset(log_data, !is.na(linker.linkerConfiguration.dateComparator.methodName))
    } 
    if ("3" %in% input$logLinkerGroup) {
      log_data <- subset(log_data, !is.na(linker.linkerConfiguration.serviceReferenceComparator.methodName))
    }
    
    # Filter by source files
    if (!is.null(input$select_source)) {
      #print(typeof(input$select_source))
      log_data <- subset(log_data, source.name %in% input$select_source | target.name %in% input$select_source)
    }

    name_list <- as.vector(log_data$linker.linkerCollectionName)
    name_list <- sort(name_list, decreasing=TRUE)

        # Depending on input$input_type, we'll generate a different
    # UI component and send it to the client.
    selectInput("select", label = h3("Select Results"), width = 350,
                 choices = name_list, selected = name_list[1])
  })
  
  # Populate field for filtering by source file
  output$source_file_select <- renderUI({
    log_data <- all_log_data
    sources <- unique(log_data$source.name)
    targets <- unique(log_data$target.name)
    selectInput("select_source", label = h3("Filter by Source Files"), choices = union(sources, targets), selected = NULL, multiple = TRUE)
  })
  
  # All of the below populate the data tabs
  # They each follow the same pattern:
  ## React to drop down refresh
  ## Update logging and source data files
  ## Update data from Mongo
  ## Filter results based on score filter values and a tab specific tab (e.g. links with different surnames)
  ## Filter top N based on records returned filter
  ## Results are saved to a global variable which is used elsewhere for retrieving individual scores
  output$diffsurs <- DT::renderDataTable({
    datafile <- input$select
    slope <- input$slope
    
    logging()
    st <- system.time(data())
    print(st)
    update_confidence()
    score_range <- input$range
    minscore <- score_range[1]
    maxscore <- score_range[2]
    #print(nrow(maxscore))
    sub_res <- subset(m_results, Score >= minscore & Score <= maxscore & tolower(Surnames_A) != tolower(Surnames_B))
    topn <- head(sub_res[with(sub_res, order(-Score)),],input$records)
    current_results <<- topn
    print(display_columns)
    subset(topn, select = display_columns)
    #topn
    
  }, server = FALSE,  escape = FALSE)

  # This function is also repeated below and displays score breakdown for any records selected in the data table
  output$diffsur_scores <- renderPrint({
    printscores(input$diffsurs_rows_selected)
    
  })
    
  output$difffores <- DT::renderDataTable({
    datafile <- input$select
    logging()
    data()
    update_confidence()
    sub_res <- subset(m_results, tolower(Forenames_A) != tolower(Forenames_B))
    topn <- head(sub_res[with(sub_res, order(-Score)),],input$records)
    current_results <<- topn
    subset(topn, select = display_columns)
    
  }, server = FALSE,  escape = FALSE)

  output$difffores_scores <- renderPrint({
    printscores(input$difffores_rows_selected)
  })  
  
  output$diffdobs <- DT::renderDataTable({
    datafile <- input$select
    logging()
    data()
    update_confidence()
    sub_res <- subset(m_results, DOB_A != DOB_B)
    topn <- head(sub_res[with(sub_res, order(-Score)),],input$records)
    current_results <<- topn
    subset(topn, select = display_columns)
    #topn
    
  }, server = FALSE, escape = FALSE)  
  
  output$diffdobs_scores <- renderPrint({
    printscores(input$diffdobs_rows_selected)
  })
    
  output$diffsnums <- DT::renderDataTable({

    datafile <- input$select
    logging()
    data()
    update_confidence()
    sub_res <- subset(m_results, ServNum_A != ServNum_B)
    topn <- head(sub_res[with(sub_res, order(-Score)),],input$records)
    current_results <<- topn
    subset(topn, select = display_columns)
    
  }, server = FALSE, escape = FALSE)
  
  output$diffsnums_scores <- renderPrint({
    printscores(input$diffsnums_rows_selected)
  })  

  output$randomsample <- DT::renderDataTable({
    logging()
    datafile <- input$select
    data()
    update_confidence()
    #sub_res <- subset(m_results, select = display_columns)
    samp_size <- input$records
    if (samp_size > nrow(m_results)) {
      samp_size <- nrow(m_results)
    }
    
    topn <- m_results[sample(nrow(m_results),samp_size),]
    current_results <<- topn
    subset(topn, select = display_columns)
    
  }, server = FALSE, escape = FALSE)
  
  output$diffrand_scores <- renderPrint({
    printscores(input$randomsample_rows_selected)
  })  
  
  output$diffsample <- DT::renderDataTable({
    datafile <- input$select
    logging()
    data()
    update_confidence()
    sub_res <- subset(m_results, tolower(Forenames_A) != tolower(Forenames_B) | tolower(Surnames_A) != tolower(Surnames_B))
    samp_size <- input$records
    if (samp_size > nrow(sub_res)) {
       samp_size <- nrow(sub_res)
    }
    
    topn <- sub_res[sample(nrow(sub_res),samp_size),]
    current_results <<- topn
    subset(topn, select = display_columns)
    
  }, server = FALSE, escape = FALSE)
  
  output$diffsample_scores <- renderPrint({
    printscores(input$diffsample_rows_selected)
  })  
  
}
