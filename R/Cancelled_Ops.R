##libraries
library(curl)
library(tidyverse)
library(dplyr)
library(lubridate)                        
library(readr)   
library(Rcpp)
library(sf)
library(ggmap)
library(data.table)

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


## Health board codes 
temp2 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/9f942fdb-e59e-44f5-b534-d6e17229cc7b/resource/652ff726-e676-4a20-abda-435b98dd7bdc/download/hb14_hb19.csv"
temp2 <- curl_download(url=source, destfile=temp2, quiet=FALSE, mode="wb")

healthboardid <- read.csv(temp2)
healthboardid <- healthboardid[c("HB","HBName")]

names(healthboardid)[names(healthboardid) == 'HB'] <- 'HBT'

## Special health board codes
temp3 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/65402d20-f0f1-4cee-a4f9-a960ca560444/resource/0450a5a2-f600-4569-a9ae-5d6317141899/download/special-health-boards_19022021.csv"
temp3 <- curl_download(url=source, destfile=temp3, quiet=FALSE, mode="wb")

specialid <- read.csv(temp3)
names(specialid)[names(specialid) == 'SHB'] <- 'HBT'
names(specialid)[names(specialid) == 'SHBName'] <- 'HBName'

specialid <- specialid[c("HBT","HBName")]


##country code 
ScotID <- data.frame("S92000003", "Scotland")  
names(ScotID) <- c("HBT", "HBName")


#combine lookup codes & join
lookups <- rbind(ScotID, healthboardid, specialid)

cancelledops <- cancelledops %>% 
  left_join(lookups)


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
  mutate(Cancelled_By_Patient_pc_of_planned_ops=round(CancelledByPatientReason/TotalOperations*100, digit = 1)) %>%
  mutate(Cancelled_clinical_reason_pc_of_planned_ops=round(ClinicalReason/TotalOperations*100, digit = 1)) %>%
  mutate(Non_clinical_capacity_reason_pc_of_planned_ops=round(NonClinicalCapacityReason/TotalOperations*100, digit =1)) %>%
  mutate(Other_Reason_pc_of_planned_ops=round(OtherReason/TotalOperations*100, digit = 1)) %>%
  mutate(Cancelled_By_Patient_pc_of_cancelled_ops=round(CancelledByPatientReason/TotalCancelled*100, digit = 1)) %>%
  mutate(Cancelled_clinical_reason_pc_of_cancelled_ops=round(ClinicalReason/TotalCancelled*100, digit = 1)) %>%
  mutate(Non_clinical_capacity_reason_pc_of_cancelled_ops=round(NonClinicalCapacityReason/TotalCancelled*100, digit = 1)) %>%
  mutate(Other_Reason_pc_of_cancelled_ops=round(OtherReason/TotalOperations*100, digit = 1)) %>%
  mutate(Performed_PC=round(Performed/TotalOperations*100, digit = 1)) %>%
  mutate(Cancelled_PC=round(TotalCancelled/TotalOperations*100, digit = 1)) %>%
  mutate(PerformedPopUp=Performed) %>%
  mutate(CancelledPopUp=TotalCancelled)

# selected columns
cancelledops <- cancelledops[c("Date2", "Month","Year","HBT","HBName","TotalOperations",
                               "TotalCancelled","Performed","Performed_PC", "Cancelled_PC",
                               "CancelledByPatientReason",
                               "ClinicalReason","NonClinicalCapacityReason","OtherReason",
                               "Cancelled_By_Patient_pc_of_planned_ops","Cancelled_clinical_reason_pc_of_planned_ops",
                               "Non_clinical_capacity_reason_pc_of_planned_ops","Other_Reason_pc_of_planned_ops",
                               "Cancelled_By_Patient_pc_of_cancelled_ops","Cancelled_clinical_reason_pc_of_cancelled_ops",
                               "Non_clinical_capacity_reason_pc_of_cancelled_ops","Other_Reason_pc_of_cancelled_ops", 
                               "PerformedPopUp", "CancelledPopUp")]

names(cancelledops)[names(cancelledops) == 'TotalCancelled'] <- 'Cancelled'
names(cancelledops)[names(cancelledops) == 'CancelledByPatientReason'] <- 'Cancelled by patient'
names(cancelledops)[names(cancelledops) == 'NonClinicalCapacityReason'] <- 'Capacity reason'
names(cancelledops)[names(cancelledops) == 'OtherReason'] <- 'Other reason'
names(cancelledops)[names(cancelledops) == 'ClinicalReason'] <- 'Clinical reason'

##reorder scotland first

cancelledops$HBT <- factor(cancelledops$HBT, levels = c("S92000003", "S08000015", "S08000016", "S08000017",
                                        "S08000018", "S08000019", "S08000020", "S08000021",
                                        "S08000022", "S08000023", "S08000024", "S08000025",
                                        "S08000026", "S08000027", "S08000028", "S08000029",
                                        "S08000030", "S08000031", "S08000032")) 

HB_order <- c("S92000003", "S08000015", "S08000016", "S08000017",
              "S08000018", "S08000019", "S08000020", "S08000021",
              "S08000022", "S08000023", "S08000024", "S08000025",
              "S08000026", "S08000027", "S08000028", "S08000029",
              "S08000030", "S08000031", "S08000032")

cancelledops <- cancelledops[ order(match(cancelledops$HBT, HB_order)), ]



## 3. Cancelled operations by hospital

temp4 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/479848ef-41f8-44c5-bfb5-666e0df8f574/resource/bcc860a4-49f4-4232-a76b-f559cf6eb885/download/cancellations_by_hospital_march_2022.csv"
temp4 <- curl_download(url=source, destfile=temp4, quiet=FALSE, mode="wb")

CBHOS <- read.csv(temp4)

## Hospital locations (open only)

temp5 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/cbd1802e-0e04-4282-88eb-d7bdcfb120f0/resource/c698f450-eeed-41a0-88f7-c1e40a568acc/download/current-hospital_flagged20211216.csv"
temp5 <- curl_download(url=source, destfile=temp5, quiet=FALSE, mode="wb")

currenthospitals <- read.csv(temp5)

## A&E sites

temp6 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/a877470a-06a9-492f-b9e8-992f758894d0/resource/1a4e3f48-3d9b-4769-80e9-3ef6d27852fe/download/hospital_site_list.csv"
temp6 <- curl_download(url=source, destfile=temp6, quiet=FALSE, mode="wb")

aesites <- read.csv(temp6)

## join

names(currenthospitals)[names(currenthospitals) == 'Location'] <- 'TreatmentLocationCode'

hospitalslookup <- currenthospitals %>%
  left_join(aesites)


## convert to lat / long
## excludes NA coordinates

hospitalslookup <- hospitalslookup[c("TreatmentLocationCode","LocationName", 
                                     "Postcode","AddressLine", "HB","CA","XCoordinate",
                                     "YCoordinate", "CurrentDepartmentType",
                                     "Comments", "Status")] %>%
  filter(!is.na(XCoordinate)| !is.na(YCoordinate))

hospitalslookup <- st_as_sf(hospitalslookup, coords = c("XCoordinate", "YCoordinate"), crs = 7405) ##EPSG:7405: OSGB36 / British National Grid + ODN height

hospitalslookup <- st_transform(hospitalslookup, crs = 4326 ) 
hospitalslookup <- hospitalslookup %>%
  mutate( lon = st_coordinates(hospitalslookup)[,1],
          lat = st_coordinates(hospitalslookup)[,2])


## council areas

temp7 <- tempfile()
source <- "https://www.opendata.nhs.scot/dataset/9f942fdb-e59e-44f5-b534-d6e17229cc7b/resource/967937c4-8d67-4f39-974f-fd58c4acfda5/download/ca11_ca19.csv"
temp7 <- curl_download(url=source, destfile=temp7, quiet=FALSE, mode="wb")

CA <- read.csv(temp7)

CA <- CA %>%
  filter(is.na(CADateArchived)) %>%
  filter(is.na(HSCPDateArchived))%>%
  filter(is.na(HBDateArchived))

CA <- CA[c("CA","CAName")]

##joins

hospitalslookup <- hospitalslookup %>%
  left_join(CA)

names(CBHOS)[names(CBHOS) == 'Hospital'] <- 'TreatmentLocationCode'


CBHOS <- CBHOS %>% 
  left_join(hospitalslookup) 

##date formatting

names(CBHOS)[names(CBHOS) == 'Month'] <- 'Date1'
CBHOS <- CBHOS %>%
  mutate(Date2=as.Date(paste0(as.character(Date1), '01'), format='%Y%m%d')) %>%
  mutate(Year=format(as.Date(Date2, format="%d/%m/%Y"),"%Y")) %>%
  mutate(Month=format(as.Date(Date2, format="%d/%m/%Y"),"%b"))


##calculated fields: performed (planned - cancelled), and % calculations 
##(cancellations by type as % of all planned ops and as a % of cancelled operations)

CBHOS <- CBHOS %>%
  mutate(Performed=TotalOperations-TotalCancelled) %>%
  mutate(Cancelled_By_Patient_pc_of_planned_ops=round(CancelledByPatientReason/TotalOperations*100, digit = 1)) %>%
  mutate(Cancelled_clinical_reason_pc_of_planned_ops=round(ClinicalReason/TotalOperations*100, digit = 1)) %>%
  mutate(Non_clinical_capacity_reason_pc_of_planned_ops=round(NonClinicalCapacityReason/TotalOperations*100, digit =1)) %>%
  mutate(Other_Reason_pc_of_planned_ops=round(OtherReason/TotalOperations*100, digit = 1)) %>%
  mutate(Cancelled_By_Patient_pc_of_cancelled_ops=round(CancelledByPatientReason/TotalCancelled*100, digit = 1)) %>%
  mutate(Cancelled_clinical_reason_pc_of_cancelled_ops=round(ClinicalReason/TotalCancelled*100, digit = 1)) %>%
  mutate(Non_clinical_capacity_reason_pc_of_cancelled_ops=round(NonClinicalCapacityReason/TotalCancelled*100, digit = 1)) %>%
  mutate(Other_Reason_pc_of_cancelled_ops=round(OtherReason/TotalOperations*100, digit = 1)) %>%
  mutate(Performed_PC=round(Performed/TotalOperations*100, digit = 1)) %>%
  mutate(Cancelled_PC=round(TotalCancelled/TotalOperations*100, digit = 1)) %>%
  mutate(Group=Cancelled_PC) %>%
  mutate(PerformedPopUp=Performed) %>%
  mutate(CancelledPopUp=TotalCancelled)

## HB names are missing?
## new data frame with selected columns
CBHOS <- CBHOS[c("Date2","Month","Year","LocationName", "TreatmentLocationCode", "CurrentDepartmentType", 
                 "Status",
                 "Postcode", "AddressLine", "lon", "lat", "CAName", "TotalOperations",
                 "TotalCancelled","Performed","Performed_PC", "Cancelled_PC", "Group",
                 "CancelledByPatientReason",
                 "ClinicalReason","NonClinicalCapacityReason","OtherReason",
                 "Cancelled_By_Patient_pc_of_planned_ops","Cancelled_clinical_reason_pc_of_planned_ops",
                 "Non_clinical_capacity_reason_pc_of_planned_ops","Other_Reason_pc_of_planned_ops",
                 "Cancelled_By_Patient_pc_of_cancelled_ops","Cancelled_clinical_reason_pc_of_cancelled_ops",
                 "Non_clinical_capacity_reason_pc_of_cancelled_ops","Other_Reason_pc_of_cancelled_ops", 
                 "PerformedPopUp", "CancelledPopUp")]

names(CBHOS)[names(CBHOS) == 'TotalCancelled'] <- 'Cancelled'
names(CBHOS)[names(CBHOS) == 'CancelledByPatientReason'] <- 'Cancelled by patient'
names(CBHOS)[names(CBHOS) == 'NonClinicalCapacityReason'] <- 'Capacity reason'
names(CBHOS)[names(CBHOS) == 'OtherReason'] <- 'Other reason'
names(CBHOS)[names(CBHOS) == 'ClinicalReason'] <- 'Clinical reason'


##filter by status and create file names
##CBHOS <- CBHOS %>%
##  filter(Status=='Open') %>%
##  mutate(filename=gsub(" ", "", TreatmentLocationName))  


## filter to latest date for map base
hospitalmapbase <- CBHOS %>% 
  group_by(TreatmentLocationCode) %>%
  filter(Date2==max(Date2))

## set groups and lookup flourish marker icons 


markerlookup <- data.frame(Group=rep(c('0-10', '10-20', '30-40', '0-50', '50+')),
                 Marker=rep(c('https://public.flourish.studio/uploads/654810/6e42adcc-51f4-4a13-9a1b-2f4cb1c18b89.png', 
                           'https://public.flourish.studio/uploads/654810/752b0d31-bca6-45d2-843c-a2b913e0aadd.png', 
                           'https://public.flourish.studio/uploads/654810/102c623a-98e7-4419-b8a1-2acd7f49d14f.png', 
                           'https://public.flourish.studio/uploads/654810/d51b7238-8264-4440-ad9c-4e7dad619021.png', 
                           'https://public.flourish.studio/uploads/654810/80fa7955-f2fd-4fb2-a6e6-75ea8901a5df.png')))
              



hospitalmapbase$Group<-c( "0-10", "10-20", "20-30", "30-40", "40-50", "50+")[
                   findInterval(hospitalmapbase$Group , c(-Inf, 9.99, 19.99, 29.99, 39.99, 49.99, Inf) ) ]

hospitalmapbase <- hospitalmapbase %>% 
  left_join(markerlookup)

##Export CSVs

## 1. cancelled ops by healthboard
write.csv(cancelledops, "data/cancelled_ops/cancelled_by_HB.csv", row.names = FALSE)

## 2. hospital map base 
write.csv(hospitalmapbase, "data/cancelled_ops/map_base.csv", row.names = FALSE) ## all hospitals

## test github
CBHOS %>% 
  group_by(TreatmentLocationCode) %>% 
  group_walk(~ write_csv(.x, paste0("data/cancelled_ops/hospitals/", .y$TreatmentLocationCode, ".csv")))


##write.csv(res[[2]], 
           ## paste0("output-data/alternative-exports-by-non-iso-week/excess-deaths/", country, "_excess_deaths.csv"),
            ##fileEncoding = "UTF-8",row.names=FALSE)








