
### --- Setup
library(data.table)
library(dplyr)
library(stringi)
setwd('/data')

### --- Edit Conflicts data: Collect data from log.EditConflict_8860941

# - SQL from log.EditConflict_8860941 > write locally to .tsv
system(
  command = 'mysql --defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-slave.eqiad.wmnet -e 
  "SELECT * FROM log.EditConflict_8860941" > 
  /home/goransm/RScripts/TechnicalWishes/TwoColConflict/data/EditConflict_07082018.tsv',
  wait = T)
# - read conflicts data
conflictsFrame <- fread('EditConflict_07082018.tsv', sep = "\t")
# - remove bots from conflicts data
conflictsFrame <- filter(conflictsFrame, 
                         grepl('is_bot": false', conflictsFrame$userAgent, fixed = T))
# - remove anonimous edits
conflictsFrame <- filter(conflictsFrame, event_userId != 0)
# - heuristic to filter IP addresses
ipv4Regex <- '((^[[:digit:]]+\\.)([[:digit:]]+\\.)+)([[:digit:]]+)'
ipv6Regex <- '((^[[:alnum:]]+:)([[:alnum:]]+:)+)([[:alnum:]]+)'
# - test heuristics
# text <- c('192.168.1.1', 'alpha')
# which(grepl(ipv4Regex, text))
# text <- c('2001:0db8:85a3:0044:1000:1a2b:0357:7337', 'alpha')
# which(grepl(ipv6Regex, text))
wipv4 <- which(grepl(ipv4Regex, conflictsFrame$event_userText))
wipv6 <- which(grepl(ipv6Regex, conflictsFrame$event_userText))
if (length(wipv4) >= 1) {conflictsFrame <- conflictsFrame[-wipv4, ]}
if (length(wipv6) >= 1) {conflictsFrame <- conflictsFrame[-wipv6, ]}
# - NOTE: we are loosing one (1) user's edit conflicts here, because:
# - "90.191.1g6.98" (the only event_userText that satisfies ipv4Regex) 
# - for some reason satisfies the ipv4Regex condition indeed.
# - Need to inspect this further.
# - group conflicts data by userId, wiki, and user_text and sum up the edits
conflictsFrame <- conflictsFrame %>% 
  select(event_userId, wiki) %>% 
  group_by(event_userId, wiki) %>% 
  summarise(conflicts = n())
conflictsFrame <- as.data.frame(conflictsFrame)

### --- User edits data: using event_userText from the Conflicts data
### --- to query wmf.mediawiki_history Hive table

# - NOTE: using the 2018-06 of the wmf.mediawiki_history Hive table

# - HiveQL query:
# - formulate HiveQL query
hiveQL <- "SELECT wiki_db, event_user_id, count(*) as revision_count
  FROM wmf.mediawiki_history
  WHERE event_entity = 'revision' 
  AND event_type = 'create'
  AND event_user_is_bot_by_name = false
  AND event_user_is_anonymous = false
  AND NOT ARRAY_CONTAINS(event_user_groups, 'bot')
  AND snapshot = '2018-06' 
  GROUP BY wiki_db, event_user_id;"
scommand <-
  paste0('/usr/local/bin/beeline --incremental=true -e "', 
         hiveQL, 
         '" > /home/goransm/RScripts/TechnicalWishes/TwoColConflict/data/wmf_mediawiki_userEdits.tsv')
q <- system(scommand, wait = T)

userEdits <- fread('wmf_mediawiki_userEdits.tsv', sep = "\t")
userEdits <- as.data.frame(userEdits)

# - left_join conflictsFrame, userEdits
conflictsFrame <- left_join(conflictsFrame, userEdits, 
                            by = c("event_userId" = "event_user_id", "wiki" = "wiki_db"))
rm(userEdits); gc()

# - produce user edits groups
conflictsFrame <- arrange(conflictsFrame, desc(revision_count, conflicts))
write.csv(conflictsFrame, "conflictsFrame.csv")
conflictsFrame$revision_count[is.na(conflictsFrame$revision_count)] <- 0
conflictsFrame$EditsGroup <- sapply(conflictsFrame$revision_count, function(x) {
  if (x == 0) {
    return('G0')
  } else if (x >= 1 & x <= 10) {
      return('G1')
  } else if (x >= 11 & x <= 100) {
      return('G2')
  } else if (x >= 101 & x <= 200) {
    return('G3')
  } else if (x > 200)  {
        return('G4')
    }
})

# - produce output table
result <- conflictsFrame %>% 
  select(EditsGroup, conflicts) %>% 
  group_by(EditsGroup) %>% 
  summarise(TotalConflicts = sum(conflicts), Average = mean(conflicts), Median = median(conflicts))
result$Percentage <- 
  round(result$TotalConflicts/sum(result$TotalConflicts)*100, 2)
write.csv(result, "Conflicts_Per_Edit_Group.csv")
result



