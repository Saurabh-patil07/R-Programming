#____________________________________________________________________________________
#
#- Author name: Saurabh Patil                              
#- Last updated: 1/15/2017
#- Title of program: Demographic predictions using text mining
#- Code version: v0.0812
#- Type: R souce code
#
#- © All rights are reserved. _______________________________________________________


#--------------------------------- Use libraries ----------------------------------

library(jsonlite); library(plyr); library(dplyr); library(rvest); library(RCurl);
library(scrapeR); library(XML); library(NLP); library(tm); library(tidytext);
library(sqldf); library(qdap); library(R.utils); library(xlsx); library(ggmap);
library(rgeolocate);

#-----------------------------------------Import JSON---------------------------------------

rtb_data.training <- as_data_frame(flatten(stream_in(file("sample.json"))))

#rtb_data.training <- rtb_data.training[which(!is.na(rtb_data.training$ssp_req.site.page)),]

#-------------------------------------Keep relevant columns---------------------------------

rtb_data.training <- (rtb_data.training %>%
  select(date, name, mediatype, providerid, ssp_req.id,  ssp_req.site.id, ssp_req.site.page,
         ssp_req.site.publisher.name, ssp_req.app.id, ssp_req.app.storeurl, ssp_req.app.bundle, ssp_req.device.ifa,
         ssp_req.app.name, ssp_req.app.domain, ssp_req.app.publisher.name, ssp_req.user.buyeruid, ssp_req.user.gender,
         ssp_req.user.id, ssp_req.device.model, ssp_req.device.os, 
         ssp_req.device.devicetype, ssp_req.device.ip, ssp_req.device.ua,	ssp_req.device.dnt,
         ssp_req.device.carrier,	ssp_req.device.make, ssp_req.device.geo.country,
         ssp_req.device.geo.lon, ssp_req.device.geo.lat,	
         ssp_req.device.geo.city, ssp_req.device.geo.zip, ssp_req.device.geo.region, ssp_req.device.geo.metro	
        ))

#--------------------------------------Web scrapping----------------------------------------

IAB_cat <- read.csv("IAB_categories.csv", header = TRUE)
df.lib.category <- read.csv(".\\Libraries\\Categories.csv", header = TRUE)
lib.gender <- read.csv(".\\Libraries\\Gender.csv", header = TRUE)
cat.score <- read.csv("category.csv", header = TRUE)
Stats.interest <- data.frame(category= character(22),count=rep(0,22),
                             Total_Freq=rep(0,22), stringsAsFactors = FALSE)

Stats.interest$category <- c('Gaming','Sports','Business','Finance','Education','Movies',
                             'Music', 'Travel', 'Hotels', 'Automotive', 'Careers','Family',
                             'Health & fitness', 'Food & drink', 'Photography','Technology',
                             'Politics', 'Social media', 'Shopping', 'Style & fashion',
                             'Real estate', 'Arts')

namevector <- c("Custom_keywords","IAB_categories_matched","content_category","content_category2","df.category", 
                "gender", "continent_name","country_name","country_code","region_name","city_name","timezone")
rtb_data.training[,namevector] <- NA
remove(namevector)

for (i in 1:10337){ print(i)
  toSpace <- content_transformer(function(x, pattern) gsub(pattern, " ", x))
  if(grepl("https://itunes.apple.com",rtb_data.training$ssp_req.app.storeurl[i])){
    next
  } else if(is.na(rtb_data.training$ssp_req.app.storeurl[i]) && is.na(rtb_data.training$ssp_req.site.page[i])){
    url <- paste("https://play.google.com/store/apps/details?id=", rtb_data.training$ssp_req.app.domain[i], sep = "")
    var.html <- try(evalWithTimeout(getURL(url, followlocation = TRUE, .encoding = 'UTF-8'),
                                   timeout = 10, onTimeout = "error"))
    html_url <- strsplit(url ,"http:|www.|.com/|-|/|.html")
    if(inherits(var.html,"try-error")) next
  } else if(is.na(rtb_data.training$ssp_req.site.page[i]) || rtb_data.training$ssp_req.site.page[i] == ''){
    url <- rtb_data.training$ssp_req.app.storeurl[i]
    var.html <- try(evalWithTimeout(getURL(url, followlocation = TRUE, .encoding = 'UTF-8'),
                                timeout = 10, onTimeout = "error"))
    if(inherits(var.html,"try-error")) next
    html_url <- strsplit(url ,"http:|www.|.com/|-|/|.html")
  } else if(!is.na(rtb_data.training$ssp_req.site.page[i])) {
    url <- rtb_data.training$ssp_req.site.page[i]
    var.html <- try(evalWithTimeout(getURL(url, followlocation = TRUE, .encoding = 'UTF-8'),
                                timeout = 10, onTimeout = "error"))
    if(inherits(var.html,"try-error")) next
    html_url <- strsplit(url ,"http:|www.|.com/|-|/|.html")
  } else if(!exists("var.html")) next
    
if (grepl("Page not found", var.html)| grepl("Request unsuccessful", var.html) | grepl("400 Bad Request", var.html)) next
    
  
  doc = try(htmlParse(var.html[1], asText=TRUE))
  if(inherits(doc,"try-error")) next
  contents <- try(xpathSApply(doc, "//text()[not(ancestor::script)][not(ancestor::style)][not(ancestor::noscript)][not(ancestor::form)]", xmlValue))
  if(inherits(contents,"try-error")) next
  contents <- try(sapply(contents, function(txt) paste(txt, collapse = " ")))
  if(inherits(contents,"try-error")) next
  contents <- sapply(contents,function(row) iconv(row, "latin1", "ASCII", sub=""))
  contents <- Corpus(VectorSource(contents))
  contents <- tm_map(contents, toSpace, "/|@|\\|\n")
  contents <- tm_map(contents, content_transformer(tolower))
  contents <- tm_map(contents, removeNumbers)
  contents <- tm_map(contents, removePunctuation)
  contents <- tm_map(contents, removeWords, stopwords("english"))
  contents <- tm_map(contents, removeWords,
                     c("free","back","can","review","get","dont", "llc","/n"))
  if(grepl("https://play.google.com/store",rtb_data.training$ssp_req.app.storeurl[i]) | grepl("https://play.google.com/store",rtb_data.training$ssp_req.site.page[i])){
  contents <- tm_map(contents, removeWords, c("google", "app", "apps", "play", "store", "android"))
  }
  contents <- tm_map(contents, stripWhitespace)
  #capture.output(cat(paste(contents, collapse = " ")), file = 'test2.txt')
  dtm_cont <- DocumentTermMatrix(contents)
  freq_cont <- colSums(as.matrix(dtm_cont))
  freq_cont <-sort(freq_cont, decreasing=TRUE)
  if(is.null(dtm_cont$dimnames$Terms)) next
  FreqMat_cont <- data.frame(Words_content = names(freq_cont), Frequency_content = freq_cont, Weight = 2)
  #o <- check_spelling(FreqMat_cont$Words_content, 2, TRUE)
  #FreqMat_cont <- FreqMat_cont[!(FreqMat_cont$Words_content %in% o$not.found),]
  #write.csv(FreqMat_cont, file="dtm_cont.csv")
  if (nrow(FreqMat_cont)< 10) next
  remove(contents, doc, dtm_cont, freq_cont, url)
  
  meta_desc <- try(var.html %>%
    as.character() %>%
    read_html() %>%
    html_nodes(xpath = '//meta[@name="description"]') %>%
    html_attr('content'))
  if(inherits(meta_desc,"try-error")) next
  meta_key <- try(var.html %>%
    as.character() %>%
    read_html() %>%
    html_nodes(xpath = '//meta[@name="keywords"]') %>%
    html_attr('content'))
  if(inherits(meta_key,"try-error")) next
  meta_ <- rbind(meta_key, meta_desc)
  html_meta <- sapply(meta_, function(txt) paste(txt, collapse = " "))
  html_meta <- sapply(html_meta,function(row) iconv(row, "latin1", "ASCII", sub=""))
  html_meta <- Corpus(VectorSource(html_meta))
  html_meta <- tm_map(html_meta, toSpace, "/|@|\\|")
  html_meta <- tm_map(html_meta, content_transformer(tolower))
  html_meta <- tm_map(html_meta, removeNumbers)
  html_meta <- tm_map(html_meta, removePunctuation)
  html_meta <- tm_map(html_meta, removeWords, stopwords("english"))
  html_meta <- tm_map(html_meta, stripWhitespace)
  html_meta <- tm_map(html_meta, removeWords, 
                      c("text", "html", "charsetutf", "ieedge", "chrome", "jpg", "usd", "https", "apps", "app","website", "google","http", "wordpress","article"))
  if(grepl("https://play.google.com/store",rtb_data.training$ssp_req.app.storeurl[i]) | grepl("https://play.google.com/store",rtb_data.training$ssp_req.site.page[i])){
    html_meta <- tm_map(html_meta, removeWords, c("google", "app", "apps", "play", "store", "android"))
  }
  #capture.output(cat(paste(html_meta, collapse = " ")), file = 'test3.txt')
  dtm_meta <- DocumentTermMatrix(html_meta)
  FreqMat_meta <- data.frame()
  if(!(is.null(dtm_meta$dimnames$Terms))){
    freq_meta <- colSums(as.matrix(dtm_meta))
    freq_meta <-sort(freq_meta, decreasing=TRUE)
    FreqMat_meta <- data.frame(Words_content = names(freq_meta), Frequency_content = freq_meta, Weight = 5)
 #   o <- check_spelling(FreqMat_meta$Words_content, 2, TRUE)
 #   FreqMat_meta <- FreqMat_meta[!(FreqMat_meta$Words_content %in% o$not.found),]
    #write.csv(FreqMat_meta, file="dtm_meta.csv") 
  }
  remove(meta_desc, meta_key, html_meta, dtm_meta, freq_meta)
  
  html_title <- try(var.html %>%
    as.character() %>%
    read_html() %>%
    html_nodes(xpath = '//title'))
  if(inherits(html_title,"try-error")) next
  html_title <- sapply(html_title,function(row) iconv(row, "latin1", "ASCII", sub=""))
  html_title <- Corpus(VectorSource(html_title))
  html_title <- tm_map(html_title, toSpace, "<title>")
  html_title <- tm_map(html_title, toSpace, "</title>\n")
  html_title <- tm_map(html_title, toSpace, "/|@|\\|")
  html_title <- tm_map(html_title, content_transformer(tolower))
  html_title <- tm_map(html_title, removeNumbers)
  if(grepl("https://play.google.com/store",rtb_data.training$ssp_req.app.storeurl[i]) | grepl("https://play.google.com/store",rtb_data.training$ssp_req.site.page[i])){
    html_title <- tm_map(html_title, removeWords, c("google", "app", "apps", "play", "store", "android"))
  }
  html_title <- tm_map(html_title, removePunctuation)
  html_title <- tm_map(html_title, removeWords, stopwords("english"))
  html_title <- tm_map(html_title, stripWhitespace)
  #html_title <- tm_map(html_title, removeWords, c(" list()"))
  #capture.output(cat(paste(html_title, collapse = " ")), file = 'test4.txt')
  dtm_title <- DocumentTermMatrix(html_title)
  FreqMat_title <- data.frame()
  if(!(is.null(dtm_title$dimnames$Terms))){
    freq_title <- colSums(as.matrix(dtm_title))
    freq_title <-sort(freq_title, decreasing=TRUE)
    FreqMat_title <- data.frame(Words_content = names(freq_title), Frequency_content = freq_title, Weight = 4)
 #   o <- check_spelling(FreqMat_title$Words_content, 2, TRUE)
 #   FreqMat_title <- FreqMat_title[!(FreqMat_title$Words_content %in% o$not.found),]
    #write.csv(FreqMat_title, file="dtm_title.csv")
  }
  remove(freq_title, dtm_title, html_title, toSpace, var.html)
  
  html_url <- Corpus(VectorSource(html_url))
  html_url <- tm_map(html_url, content_transformer(tolower))
  html_url <- tm_map(html_url, removeNumbers)
  html_url <- tm_map(html_url, removePunctuation)
  html_url <- tm_map(html_url, removeWords, stopwords("english"))
  html_url <- tm_map(html_url, removeWords, c(""))
  if(grepl("https://play.google.com/store",rtb_data.training$ssp_req.app.storeurl[i]) | grepl("https://play.google.com/store",rtb_data.training$ssp_req.site.page[i])){
    html_url <- tm_map(html_url, removeWords, c("google", "app", "apps", "play", "store", "android"))
  }
  html_url <- tm_map(html_url, stripWhitespace)
  dtm_url <- DocumentTermMatrix(html_url)
  freq_url <- colSums(as.matrix(dtm_url))
  freq_url <-sort(freq_url, decreasing=TRUE)
  FreqMat_url <- try(data.frame(Words_content = names(freq_url), Frequency_content = freq_url, Weight = 3))
  if(inherits(FreqMat_url,"try-error")) next
  #o <- try(check_spelling(FreqMat_url$Words_content, 2, TRUE))
  #if(inherits(o,"try-error")) next
  #FreqMat_url <- try(FreqMat_url[!(FreqMat_url$Words_content %in% o$not.found),])
  #if(inherits(FreqMat_url,"try-error")) next
  #write.csv(FreqMat_url, file="dtm_url.csv")
  remove(freq_url, dtm_url, html_url)
  
#---------------------------------------Content Analysis--------------------------------------
  
  #Giving weights in Content
  for (j in 1:length(FreqMat_cont$Words_content)){
    if((FreqMat_cont$Words_content[j] %in% FreqMat_meta$Words_content) & (nrow(FreqMat_meta)!=0)){
      FreqMat_cont$Weight[j] = 5 * 4
    }
    if((FreqMat_cont$Words_content[j] %in% FreqMat_title$Words_content) & (nrow(FreqMat_title)!=0)){
      FreqMat_cont$Weight[j] = FreqMat_cont$Weight[j] * 3
    }
    if((FreqMat_cont$Words_content[j] %in% FreqMat_url$Words_content)){
      FreqMat_cont$Weight[j] = FreqMat_cont$Weight[j] * 2
    }
  }
  
  #Giving weights in Meta
  if(nrow(FreqMat_meta)!=0){
    for (j in 1:length(FreqMat_meta$Words_content)){
      if(FreqMat_meta$Words_content[j] %in% FreqMat_title$Words_content & !(FreqMat_meta$Words_content[j] %in% FreqMat_cont$Words_content) & (nrow(FreqMat_title)!=0)){
        FreqMat_meta$Weight[j] = 4 * 3
      }
      if(FreqMat_meta$Words_content[j] %in% FreqMat_url$Words_content & !(FreqMat_meta$Words_content[j] %in% FreqMat_cont$Words_content)){
        FreqMat_meta$Weight[j] = FreqMat_meta$Weight[j] * 2
      }
    }
  }
  
  #Giving weights in Title
  if(nrow(FreqMat_title)!=0){
    for (j in 1:length(FreqMat_title$Words_content)){
      if(FreqMat_title$Words_content[j] %in% FreqMat_url$Words_content & !(FreqMat_title$Words_content[j] %in% FreqMat_cont$Words_content | FreqMat_title$Words_content[j] %in% FreqMat_meta$Words_content) & (nrow(FreqMat_meta)!=0)){
        FreqMat_title$Weight[j] = 3 * 2
      }
    }
  }
  
  All_words <- rbind.data.frame(FreqMat_meta, FreqMat_title, FreqMat_url, FreqMat_cont[which(FreqMat_cont$Frequency_content > 1),])
  All_words <- All_words[order(-All_words$Weight,-All_words$Frequency_content),]
  #write.csv(All_words, file="Keyword_Website.csv")
  #Get 20 keywords
  Keywords <- rbind(FreqMat_meta, FreqMat_title, FreqMat_url)
  Keywords <- Keywords[order(-Keywords$Weight),]
  Keywords <- Keywords[which( Keywords$Weight > 5),]
  if(length(Keywords$Words_content) < 20){
    count = 20-length(Keywords$Words_content)
    for(l in 1:length(FreqMat_cont$Words_content)){
      if(!(FreqMat_cont$Words_content[l] %in% Keywords$Words_content)){
        Keywords<- rbind(Keywords, FreqMat_cont[l,])
        Keywords <- Keywords[order(-Keywords$Weight, -Keywords$Frequency_content),]
        count=count-1
        if (count< 1) { remove(count, j, l); break; }
      }
    }
  }
  
  rtb_data.training$Custom_keywords[i] <- list(as.character(Keywords$Words_content))
  remove(Keywords, FreqMat_cont, FreqMat_meta, FreqMat_title, FreqMat_url)

#----------------------------------IAB Category Extraction----------------------------------
  
  #IAB_cat <- read.csv("IAB_categories.csv", header = TRUE)
  IAB_matched <- data.frame(sqldf("SELECT IAB_cat.value
                                  FROM IAB_cat
                                  INNER JOIN All_words
                                  ON IAB_cat.category = All_words.Words_content"))
  
  rtb_data.training$IAB_categories_matched[i] <- list(as.character(IAB_matched$Value))
  remove(IAB_matched)
  
#---------------------------- Category Identification for web URLs -----------------------------
 
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Gaming){
      Stats.interest$count[1] = Stats.interest$count[1] + 1
      Stats.interest$Total_Freq[1] =  Stats.interest$Total_Freq[1] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[1] = Stats.interest$Total_wt[1] + All_words$Weight[k]
    }
  }
  Stats.interest$count[1] <- (Stats.interest$count[1]/length(df.lib.category$Gaming))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Sports){ 
      Stats.interest$count[2] = Stats.interest$count[2] + 1
      Stats.interest$Total_Freq[2] = Stats.interest$Total_Freq[2] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[2] = Stats.interest$Total_wt[2] + All_words$Weight[k]
    }
  }
  Stats.interest$count[2] <- (Stats.interest$count[2]/length(df.lib.category$Sports))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Business){ 
      Stats.interest$count[3] = Stats.interest$count[3] + 1
      Stats.interest$Total_Freq[3] = Stats.interest$Total_Freq[3] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[3] = Stats.interest$Total_wt[3] + All_words$Weight[k]
    }
  }
  Stats.interest$count[3] <- (Stats.interest$count[3]/length(df.lib.category$Business))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Finance){ 
      Stats.interest$count[4] = Stats.interest$count[4] + 1
      Stats.interest$Total_Freq[4] = Stats.interest$Total_Freq[4] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[4] = Stats.interest$Total_wt[4] + All_words$Weight[k]
    }
  }
  Stats.interest$count[4] <- (Stats.interest$count[4]/length(df.lib.category$Finance))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Education){ 
      Stats.interest$count[5] = Stats.interest$count[5] + 1
      Stats.interest$Total_Freq[5] = Stats.interest$Total_Freq[5] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[5] = Stats.interest$Total_wt[5] + All_words$Weight[k]
    }
  }
  Stats.interest$count[5] <- (Stats.interest$count[5]/length(df.lib.category$Education))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Movies){ 
      Stats.interest$count[6] = Stats.interest$count[6] + 1
      Stats.interest$Total_Freq[6] = Stats.interest$Total_Freq[6] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[6] = Stats.interest$Total_wt[6] + All_words$Weight[k]
    }
  }
  Stats.interest$count[6] <- (Stats.interest$count[6]/length(df.lib.category$Movies))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Music){ 
      Stats.interest$count[7] = Stats.interest$count[7] + 1
      Stats.interest$Total_Freq[7] = Stats.interest$Total_Freq[7] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[7] = Stats.interest$Total_wt[7] + All_words$Weight[k]
    }
  }
  Stats.interest$count[7] <- (Stats.interest$count[7]/length(df.lib.category$Music))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Travel){ 
      Stats.interest$count[8] = Stats.interest$count[8] + 1
      Stats.interest$Total_Freq[8] = Stats.interest$Total_Freq[8] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[8] = Stats.interest$Total_wt[8] + All_words$Weight[k]
    }
  }
  Stats.interest$count[8] <- (Stats.interest$count[8]/length(df.lib.category$Travel))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Hotel){ 
      Stats.interest$count[9] = Stats.interest$count[9] + 1
      Stats.interest$Total_Freq[9] = Stats.interest$Total_Freq[9] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[9] = Stats.interest$Total_wt[9] + All_words$Weight[k]
    }
  }
  Stats.interest$count[9] <- (Stats.interest$count[9]/length(df.lib.category$Hotel))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Automotive){ 
      Stats.interest$count[10] = Stats.interest$count[10] + 1
      Stats.interest$Total_Freq[10] = Stats.interest$Total_Freq[10] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[10] = Stats.interest$Total_wt[10] + All_words$Weight[k]
    }
  }
  Stats.interest$count[10] <- (Stats.interest$count[10]/length(df.lib.category$Automotive))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Careers){ 
      Stats.interest$count[11] = Stats.interest$count[11] + 1
      Stats.interest$Total_Freq[11] = Stats.interest$Total_Freq[11] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[11] = Stats.interest$Total_wt[11] + All_words$Weight[k]
    }
  }
  Stats.interest$count[11] <- (Stats.interest$count[11]/length(df.lib.category$Careers))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Family){ 
      Stats.interest$count[12] = Stats.interest$count[12] + 1
      Stats.interest$Total_Freq[12] = Stats.interest$Total_Freq[12] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[12] = Stats.interest$Total_wt[12] + All_words$Weight[k]
    }
  }
  Stats.interest$count[12] <- (Stats.interest$count[12]/length(df.lib.category$Family))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Health){ 
      Stats.interest$count[13] = Stats.interest$count[13] + 1
      Stats.interest$Total_Freq[13] = Stats.interest$Total_Freq[13] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[13] = Stats.interest$Total_wt[13] + All_words$Weight[k]
    }
  }
  Stats.interest$count[13] <- (Stats.interest$count[13]/length(df.lib.category$Health))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Food){ 
      Stats.interest$count[14] = Stats.interest$count[14] + 1
      Stats.interest$Total_Freq[14] = Stats.interest$Total_Freq[14] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[14] = Stats.interest$Total_wt[14] + All_words$Weight[k]
    }
  }
  Stats.interest$count[14] <- (Stats.interest$count[14]/length(df.lib.category$Food))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Photography){ 
      Stats.interest$count[15] = Stats.interest$count[15] + 1
      Stats.interest$Total_Freq[15] = Stats.interest$Total_Freq[15] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[15] = Stats.interest$Total_wt[15] + All_words$Weight[k]
    }
  }
  Stats.interest$count[15] <- (Stats.interest$count[15]/length(df.lib.category$Photography))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Technology){ 
      Stats.interest$count[16] = Stats.interest$count[16] + 1
      Stats.interest$Total_Freq[16] = Stats.interest$Total_Freq[16] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[16] = Stats.interest$Total_wt[16] + All_words$Weight[k]
    }
  }
  Stats.interest$count[16] <- (Stats.interest$count[16]/length(df.lib.category$Technology))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Politics){ 
      Stats.interest$count[17] = Stats.interest$count[17] + 1
      Stats.interest$Total_Freq[17] = Stats.interest$Total_Freq[17] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[17] = Stats.interest$Total_wt[17] + All_words$Weight[k]
    }
  }
  Stats.interest$count[17] <- (Stats.interest$count[17]/length(df.lib.category$Politics))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Social_media){ 
      Stats.interest$count[18] = Stats.interest$count[18] + 1
      Stats.interest$Total_Freq[18] = Stats.interest$Total_Freq[18] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[18] = Stats.interest$Total_wt[18] + All_words$Weight[k]
    }
  }
  Stats.interest$count[18] <- (Stats.interest$count[18]/length(df.lib.category$Social_media))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Shopping){ 
      Stats.interest$count[19] = Stats.interest$count[19] + 1
      Stats.interest$Total_Freq[19] = Stats.interest$Total_Freq[19] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[19] = Stats.interest$Total_wt[19] + All_words$Weight[k]
    }
  }
  Stats.interest$count[19] <- (Stats.interest$count[19]/length(df.lib.category$Shopping))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Fashion){ 
      Stats.interest$count[20] = Stats.interest$count[20] + 1
      Stats.interest$Total_Freq[20] = Stats.interest$Total_Freq[20] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[20] = Stats.interest$Total_wt[20] + All_words$Weight[k]
    }
  }
  Stats.interest$count[20] <- (Stats.interest$count[20]/length(df.lib.category$Fashion))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Real_estate){ 
      Stats.interest$count[21] = Stats.interest$count[21] + 1
      Stats.interest$Total_Freq[21] = Stats.interest$Total_Freq[21] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[21] = Stats.interest$Total_wt[21] + All_words$Weight[k]
    }
  }
  Stats.interest$count[21] <- (Stats.interest$count[21]/length(df.lib.category$Real_estate))*100
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% df.lib.category$Arts){ 
      Stats.interest$count[22] = Stats.interest$count[22] + 1
      Stats.interest$Total_Freq[22] = Stats.interest$Total_Freq[22] + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[22] = Stats.interest$Total_wt[22] + All_words$Weight[k]
    }
  }
  Stats.interest$count[22] <- (Stats.interest$count[22]/length(df.lib.category$Arts))*100
  
  Total_key <- sum(All_words$Frequency_content)
  Stats.interest$Total_Freq <- (Stats.interest$Total_Freq/sum(All_words$Frequency_content))*100
  Stats.interest$mean <- rowMeans(Stats.interest[,2:3])
  #Stats.interest$final <- Stats.interest$mean * Stats.interest$Total_wt
  
  Category1 <- Stats.interest$category[which.max(Stats.interest$mean)]
  Category2 <- Stats.interest$category[which(Stats.interest$mean == sort(Stats.interest$mean, TRUE)[2])]
  if(all(Stats.interest$mean == 0)){ Category1 <- 'Unknown'; Category2 <- 'Unknown'; }
  rtb_data.training$content_category[i] <- Category1
  if(length(Category2)>1){ rtb_data.training$content_category2[i] <- Category2[1]
  }else { rtb_data.training$content_category2[i] <- Category2 }
  
  df.category <- data.frame(category= Stats.interest$category[which(Stats.interest$mean > max(Stats.interest$mean)*0.5)],
                            mean= Stats.interest$mean[which(Stats.interest$mean > max(Stats.interest$mean)*0.5)] , stringsAsFactors = FALSE)
  rtb_data.training$df.category[i] <- list(df.category[order(-df.category$mean),])
  remove(df.category, Total_key, k)
  Stats.interest[,2:4]=rep(0,22)
  
#--------------------------------------- Gender Prediction --------------------------------
  
  gender.algo = data.frame(name=c('IM','IF','LM','LF'), value=rep(0,4))
  male.count=0;  female.count=0;  male.freq=0; female.freq=0
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% lib.gender$Male){ 
      male.count = male.count + 1
      male.freq = male.freq + All_words$Frequency_content[k]
    }
  }
  male.count <- (male.count/length(lib.gender$Male))*100
  male.freq <- (male.freq/sum(All_words$Frequency_content))*100
  gender.algo$value[3] <- (male.count+male.freq)/2
  remove(male.count, male.freq)
  
  for(k in 1:(length(All_words$Words_content))){
    if(All_words$Words_content[k] %in% lib.gender$Female){ 
      female.count = female.count + 1
      female.freq = female.freq + All_words$Frequency_content[k]
      #Stats.interest$Total_wt[20] = Stats.interest$Total_wt[20] + All_words$Weight[k]
    }
  }
  female.count <- (female.count/length(lib.gender$Female))*100
  female.freq <- (female.freq/sum(All_words$Frequency_content))*100
  gender.algo$value[4] <- (female.count+female.freq)/2
  remove(female.count, female.freq)
  
  
  if (gender.algo$value[3]==gender.algo$value[4] & gender.algo$value[3]>0 | Category1 == 'Unknown'){
    gender <- 'Unkown'
  }else if(gender.algo$value[3]==0 & gender.algo$value[4]==0 | gender.algo$value[3]!=gender.algo$value[4]){ 
    if(length(Category2)>1){
    gender.algo$value[1]= (cat.score$male[which(cat.score == Category1)] +cat.score$male[which(cat.score == Category2[1])])/2
    gender.algo$value[2]= (cat.score$female[which(cat.score == Category1)] +cat.score$female[which(cat.score == Category2[1])])/2
    } else{
    gender.algo$value[1]= (cat.score$male[which(cat.score == Category1)] +cat.score$male[which(cat.score == Category2)])/2
    gender.algo$value[2]= (cat.score$female[which(cat.score == Category1)] +cat.score$female[which(cat.score == Category2)])/2
    }
    male.score= (gender.algo$value[1]+gender.algo$value[3])/2
    female.score=(gender.algo$value[2]+gender.algo$value[4])/2
    if(male.score > female.score){
      gender <- 'Male'
    }else if(male.score < female.score){
      gender <- 'Female'
    }else if(male.score == female.score){ 
      if(gender.algo$value[3]==0 & gender.algo$value[4]==0) gender <- 'Both'
      if(gender.algo$value[3]!=gender.algo$value[4]) gender <- 'Unknown'
    }
  }
  
  rtb_data.training$gender[i] <- gender
  remove(female.score, male.score, gender.algo, gender, Category1, Category2)
  
#-------------------------------------------- Geo location ---------------------------------------------
 
  file <- system.file("extdata","GeoIP2-Country.mmdb", package = "rgeolocate")
  rtb_data.training$continent_name[i] <- unlist(maxmind(rtb_data.training$ssp_req.device.ip[i], file, fields =  "continent_name"))
  rtb_data.training$country_name[i] <- unlist(maxmind(rtb_data.training$ssp_req.device.ip[i], file, fields = "country_name"))
  rtb_data.training$country_code[i] <- unlist(maxmind(rtb_data.training$ssp_req.device.ip[i], file, fields = "country_code"))
  
  file <- system.file("extdata","GeoIP2-City.mmdb", package = "rgeolocate")
  rtb_data.training$region_name[i] <- unlist(maxmind(rtb_data.training$ssp_req.device.ip[i], file, fields = "region_name"))
  rtb_data.training$city_name[i] <- unlist(maxmind(rtb_data.training$ssp_req.device.ip[i], file, fields = "city_name"))
  rtb_data.training$timezone[i] <- unlist(maxmind(rtb_data.training$ssp_req.device.ip[i], file, fields = "timezone"))
  
  remove(file)
  
#-------------------------------------------- Age category ----------------------------------
  
}

rtb_data.training$continent_name <- unlist(rtb_data.training$continent_name)
rtb_data.training$country_name <- unlist(rtb_data.training$country_name)
rtb_data.training$country_code <- unlist(rtb_data.training$country_code)
rtb_data.training$region_name <- unlist(rtb_data.training$region_name)
rtb_data.training$city_name <- unlist(rtb_data.training$city_name)
rtb_data.training$timezone <- unlist(rtb_data.training$timezone)

#fileConn<-file("output.json")
#y = array(1:2500)
#for(i in 2:2500){
#  y[i] <- toJSON(rtb_data.training[i,])
#}
#writeLines(y, "output.json")
#close(fileConn)

df.output <- (rtb_data.training[1:10337,] %>%
                select(-Custom_keywords,-IAB_categories_matched, -df.category))
write.csv(df.output, file="out.csv")
