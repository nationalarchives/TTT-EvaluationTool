
gendiscolink <- function(iaid) {
  url <- sprintf("<a href='http://discovery.nationalarchives.gov.uk/details/r/%s' target='_blank'>%s</a>",iaid,iaid)
  url
}


getfile <- function() {
  files <- file.info(list.files(pattern="*.csv"))
  latest <- row.names(head(files[order(as.POSIXct(files$mtime), decreasing=TRUE),],1))
  print(latest)
  filename <- as.character(latest[1])
  filename
}

readcolls <- function() {
  colls <- mongo.get.database.collections(mongo,d="ttt")
  sort(colls[grepl("_formatted$",colls)], decreasing=TRUE)
}

readlinks <- function(f, minscore, maxscore) {
  DBNS = f
  q_score <- sprintf('{"value.score":{"$gte": %.2f, "$lte": %.2f}}', minscore, maxscore)
  tmp <- mongo.find(mongo, ns=DBNS, query=q_score, limit=10000)
  data <- mongo.cursor.to.list(tmp)
  
  
  df <- data.frame(matrix(unlist(t(sapply(data,"[[",2))), nrow=length(data), byrow=F), stringsAsFactors=FALSE)
  names(df) <- c("iaidA","iaidB","linkrefA","linkrefB","score")
  df <- transform(df, score = as.numeric(score))
  df
}

getscores <- function(f) {
  readlinks(f,-2,99)
}


