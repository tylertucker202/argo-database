library(httr)

profileName = "4902377_66"

get.profile <- function(profileName){
  baseUrl = "https://argovis.colorado.edu/catalog/profiles/"
  url = paste(baseUrl, profileName, sep="")
  resp <- GET(url)
  if(resp$status_code==200) {
    profile = content(resp, "parsed")
  }
  else {
    profile = profile = content(resp, "raw")
  }
  return(profile)
}


profile = get.profile(profileName)
profile$`_id`
paramNames = names(profile)
meas <- profile$measurements
names = unique(names(unlist(meas)))
df <- data.frame(matrix(unlist(meas), nrow=length(meas), byrow=T))
colnames(df) <- names
df$profile_id = profile$`_id`

get.platform <- function(platformNumber){
  baseUrl = "https://argovis.colorado.edu/catalog/profiles/"
  url = paste(baseUrl, platformNumber, sep="")
  print(url)
  resp <- GET(url)
  if(resp$status_code==200) {
    profiles = content(resp, "parsed")
  }
  else {
    profiles = profile = content(resp, "raw")
  }
  return(profiles)
}
platformNumber = '3900737'
platformProfiles = get.platform(platformNumber)

get.monthly.profile.pos <- function(month, year){
  baseURL <- 'https://argovis.colorado.edu/selection/profiles'
  url <- paste(baseURL, '/', toString(month), '/', toString(year), sep="")
  print(url)
  resp <- GET(url)
  if(resp$status_code==200) {
    profiles <- content(resp, "parsed")
  }
  else {
    profiles <- content(resp, "raw")
  }
  return(profiles)
}
parse.pos.into.df <- function(profiles){
  df <- data.frame(profiles)
  if (nrow(df) == 0){
    return('error: no dataframes')
  }
  return(df)
}

metaDataProfs = get.monthly.profile.pos(1, 2018)
metaDf <- parse.pos.into.df(metaDataProfs)

tm = metaDataProfs[1:50]
n = unique(names(unlist(tm)))

nt = c()
metaDf = data.frame()
for (row in metaDataProfs) {
  dfr = data.frame(row)
  metaDf = merge(metaDf, dfr, all=TRUE)
}
asdf = bind_rows(nt, .id = "column_label")

metaDf = data.frame()
for (row in metaDataProfs) {
  dfr = data.frame(row)
  metaDf = merge(metaDf, dfr, all=TRUE)
}
asdf = bind_rows(nt, .id = "column_label")



metaDf = data.frame()
names = unique(names(unlist(metaDataProfs)))
# remove unwrapped station_parameters
dfNames = c()
for (name in names) {
  if (grepl('station_parameters', name) != 1) {
    dfNames = c(dfNames, name)
  }
}
dfNames <- c(dfNames, 'station_parameters')
metaDf <- data.frame(matrix(ncol = length(dfNames), nrow = 0))
colnames(metaDf) <- dfNames
for (row in metaDataProfs) {
  dfRow <- data.frame()
  newRow <- row
  rowNames <- names(row)
  for (key in dfNames) {
    if (is.na(match(key, rowNames))) {
      newRow[key] <- -999
    }
    else{
      newRow[key] <- row[key]
    }
  }
  metaDf[nrow(metaDf)+1, ] <- newRow
}
asdf = bind_rows(nt, .id = "column_label")