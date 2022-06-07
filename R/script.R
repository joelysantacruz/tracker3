##libraries
library(curl)
library(tidyverse)
library(plyr)  
library(dplyr)
library(lubridate)                        
library(readr)   


## PHS Cancelled Planned operations

## 1. Cancelled operations by health board

temp <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/479848ef-41f8-44c5-bfb5-666e0df8f574/resource/0f1cf6b1-ebf6-4928-b490-0a721cc98884/download/cancellations_by_board_february_2022.csv"
temp <- curl_download(url=source, destfile=temp, quiet=FALSE, mode="wb")

CBHB <- read.csv(temp)

## 2. Cancelled operations Scotland

temp1 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/479848ef-41f8-44c5-bfb5-666e0df8f574/resource/df65826d-0017-455b-b312-828e47df325b/download/cancellations_scotland_february_2022.csv"
temp1 <- curl_download(url=source, destfile=temp1, quiet=FALSE, mode="wb")

CS <- read.csv(temp1)

## rename column in Scotland CSV to match Healthboard csv for merge

names(CS)[names(CS) == 'Country'] <- 'HBT'

## Merge Scotland and individual boards
cancelledops <- rbind(CBHB, CS)


## Health board codes and remove extra columns for later merge with country code
temp2 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/9f942fdb-e59e-44f5-b534-d6e17229cc7b/resource/652ff726-e676-4a20-abda-435b98dd7bdc/download/hb14_hb19.csv"
temp2 <- curl_download(url=source, destfile=temp2, quiet=FALSE, mode="wb")

healthboardid <- read.csv(temp2)
healthboardid <- healthboardid[c("HB","HBName")]

## Special health board codes and rename & remove columns for later merge
temp3 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/65402d20-f0f1-4cee-a4f9-a960ca560444/resource/0450a5a2-f600-4569-a9ae-5d6317141899/download/special-health-boards_19022021.csv"
temp3 <- curl_download(url=source, destfile=temp3, quiet=FALSE, mode="wb")

specialid <- read.csv(temp3)
names(specialid)[names(specialid) == 'SHB'] <- 'HB'
names(specialid)[names(specialid) == 'SHBName'] <- 'HBName'

specialid <- specialid[c("HB","HBName")]


##country code and rename columns for merge with healthboard codes
temp4 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/9f942fdb-e59e-44f5-b534-d6e17229cc7b/resource/9c6e6c56-2697-4184-92c6-60d69c2b6792/download/geography_codes_and_labels_country.csv"
temp4 <- curl_download(url=source, destfile=temp4, quiet=FALSE, mode="wb")

countryid <- read.csv(temp4)
names(countryid)[names(countryid) == 'Country'] <- 'HB'
names(countryid)[names(countryid) == 'CountryName'] <- 'HBName'

#combine lookup codes
lookups <- rbind(countryid, healthboardid, specialid)


## Look up Health Boards

names(lookups)[names(lookups) == 'HB'] <- 'HBT'
cancelledops <- cancelledops %>% 
  inner_join(lookups)


##date formatting

names(cancelledops)[names(cancelledops) == 'Month'] <- 'Date1'
cancelledops <- cancelledops %>%
  mutate(Date2=as.Date(paste0(as.character(Date1), '01'), format='%Y%m%d')) %>%
  mutate(Year=format(as.Date(Date2, format="%d/%m/%Y"),"%Y")) %>%
  mutate(Month=format(as.Date(Date2, format="%d/%m/%Y"),"%b"))


##calculated fields: performed (planned - cancelled), and % calculations 
##(cancellations by type as % of all planned ops and as a % of canceled operations)
##create unique identifier
cancelledops <- cancelledops %>%
  mutate(Performed=TotalOperations-TotalCancelled) %>%
  mutate(Cancelled_By_Patient_pc_of_planned_ops=CancelledByPatientReason/TotalOperations*100) %>%
  mutate(Cancelled_clinical_reason_pc_of_planned_ops=ClinicalReason/TotalOperations*100) %>%
  mutate(Non_clinical_capacity_reason_pc_of_planned_ops=NonClinicalCapacityReason/TotalOperations*100) %>%
  mutate(Other_Reason_pc_of_planned_ops=OtherReason/TotalOperations*100) %>%
  mutate(Cancelled_By_Patient_pc_of_cancelled_ops=CancelledByPatientReason/TotalCancelled*100) %>%
  mutate(Cancelled_clinical_reason_pc_of_cancelled_ops=ClinicalReason/TotalCancelled*100) %>%
  mutate(Non_clinical_capacity_reason_pc_of_cancelled_ops=NonClinicalCapacityReason/TotalCancelled*100) %>%
  mutate(Other_Reason_pc_of_cancelled_ops=OtherReason/TotalOperations*100)



## new data frame with selected columns
cancelledops <- cancelledops[c("Date2", "Month","Year","HBT","HBName","TotalOperations","TotalCancelled","Performed",
                               "CancelledByPatientReason","ClinicalReason","NonClinicalCapacityReason","OtherReason","Cancelled_By_Patient_pc_of_planned_ops",
                               "Cancelled_clinical_reason_pc_of_planned_ops","Non_clinical_capacity_reason_pc_of_planned_ops","Other_Reason_pc_of_planned_ops",
                               "Cancelled_By_Patient_pc_of_cancelled_ops","Cancelled_clinical_reason_pc_of_cancelled_ops","Non_clinical_capacity_reason_pc_of_cancelled_ops","Other_Reason_pc_of_cancelled_ops")]

write.csv(cancelledops, "data/test.csv", row.names = FALSE)


## write csv 
##write.csv(cancelledops,
##            file = "data/Cancelled_Ops_by_HB.csv",
##            fileEncoding = "UTF-8", row.names=FALSE)

