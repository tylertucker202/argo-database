library(httr)

profileName = "4902377_66"

get.profile <- function(profileName){
  baseUrl = "http://206.189.72.50:3000/catalog/profiles/"
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
