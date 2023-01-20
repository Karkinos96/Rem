## Clean REM Data Script##
# {
rm(list = (ls()))

setwd("Z:\\C8505_Future_catch_policy\\Working_Area\\C8505C_Catch Data Monitoring (REM)\\Data Analysis\\Secondary\\COD Report Code")

install.packages("RPostgreSQL")
install.packages("RPostgres")
install.packages("tidyverse")
install.packages("mappplots")
install.packages("odbc")

require(RPostgreSQL)
library(DBI)
library(RODBC)
library(RPostgreSQL)
library(DBI)
library(dplyr)
require(RPostgres)
library(dplyr)
library(ggplot2)
library(data.table)
library(tidyverse)
library(odbc)
library(mapplots)


## DB Call##

con <- dbConnect(
  Postgres(),
  host = "citpgsqlbx011.postgres.database.azure.com",
  dbname = "bx011",
  port = 5432,
  user = "ana_rs@citpgsqlbx011",
  password = "FQGg_v++5=T9"
)

dbListTables(con)

###

## get the length weight conversion factors
WL.params2 <- read.table("Ref_tables\\WL.params.2014.txt", sep = "\t", header = T)

#
# change the ices areas names
WL.params2$ices_area <- as.character(WL.params2$Region)
WL.params2$ices_area[WL.params2$ices_area == "101A"] <- "27.1.a"
WL.params2$ices_area[WL.params2$ices_area == "101D"] <- "27.1.d"
WL.params2$ices_area[WL.params2$ices_area == "101G"] <- "27.1.g"
WL.params2$ices_area[WL.params2$ices_area == "102A"] <- "27.2.a"
WL.params2$ices_area[WL.params2$ices_area == "102B"] <- "27.2.b"
WL.params2$ices_area[WL.params2$ices_area == "102C"] <- "27.2.c"
WL.params2$ices_area[WL.params2$ices_area == "102D"] <- "27.2.d"
WL.params2$ices_area[WL.params2$ices_area == "102E"] <- "27.2.e"
WL.params2$ices_area[WL.params2$ices_area == "102F"] <- "27.2.f"
WL.params2$ices_area[WL.params2$ices_area == "102G"] <- "27.2.g"
WL.params2$ices_area[WL.params2$ices_area == "103A"] <- "27.3.a"
WL.params2$ices_area[WL.params2$ices_area == "104A"] <- "27.4.a"
WL.params2$ices_area[WL.params2$ices_area == "104B"] <- "27.4.b"
WL.params2$ices_area[WL.params2$ices_area == "104C"] <- "27.4.c"
WL.params2$ices_area[WL.params2$ices_area == "107A"] <- "27.7.a"
WL.params2$ices_area[WL.params2$ices_area == "107B"] <- "27.7.b"
WL.params2$ices_area[WL.params2$ices_area == "107C"] <- "27.7.c"
WL.params2$ices_area[WL.params2$ices_area == "107D"] <- "27.7.d"
WL.params2$ices_area[WL.params2$ices_area == "107E"] <- "27.7.e"
WL.params2$ices_area[WL.params2$ices_area == "107F"] <- "27.7.f"
WL.params2$ices_area[WL.params2$ices_area == "107G"] <- "27.7.g"
WL.params2$ices_area[WL.params2$ices_area == "107H"] <- "27.7.h"
WL.params2$ices_area[WL.params2$ices_area == "107i"] <- "27.7.i"
WL.params2$ices_area[WL.params2$ices_area == "107J"] <- "27.7.j"
WL.params2$ices_area[WL.params2$ices_area == "107K"] <- "27.7.k"
WL.params2$ices_area[WL.params2$ices_area == "106A"] <- "27.6.a"
WL.params2$ices_area[WL.params2$ices_area == "106B"] <- "27.6.b"
WL.params2$ices_area[WL.params2$ices_area == "106C"] <- "27.6.c"
WL.params2$ices_area[WL.params2$ices_area == "108A"] <- "27.8.a"
WL.params2$ices_area[WL.params2$ices_area == "108B"] <- "27.8.b"
WL.params2$ices_area[WL.params2$ices_area == "108C"] <- "27.8.c"
WL.params2$ices_area[WL.params2$ices_area == "109A"] <- "27.9.a"
WL.params2$ices_area[WL.params2$ices_area == "109B"] <- "27.9.b"


#### explore DB

expl_catch <- dbGetQuery(con, "SELECT  * FROM bx011.catch") # raw unsorted catch data

haul <- dbGetQuery(con, "SELECT  * FROM bx011.haul") # haul information

expl_trip <- dbGetQuery(con, "SELECT  * FROM bx011.trip") # trip information

expl_vessel <- dbGetQuery(con, "SELECT  * FROM bx011.vessel") # list of all vessels around the UK

species <- dbGetQuery(con, "SELECT  * FROM bx011.species") # list of cefas vessel species codes

### building DF###

DATE1 <- "2022-01-01" ## start date of data range
DATE2 <- "2022-06-30" ## end date of data range

# DATE1<-"2021-01-01"##start date of data range
# DATE2<-"2021-12-31"##end date of data range

# start extraction: added haul_number, month, state on March 2022
catch <- dbGetQuery(con, "SELECT  bx011.catch.catch_id, bx011.catch.haul_id, bx011.haul.haul_number,  date_part('quarter', bx011.haul.end_date) as QUARTER,  date_part('month', bx011.haul.end_date) as MONTH,
                                 bx011.haul.end_year, bx011.haul.start_date, bx011.haul.start_time,bx011.haul.end_date, bx011.haul.end_time, bx011.catch.species as Species, bx011.catch.species_comment, bx011.catch.tags,bx011.catch.state, bx011.catch.fate, bx011.catch.numberfish,
                                 bx011.catch.catchlength, bx011.catch.weight, bx011.catch.volume, bx011.catch.volume_units,
                                 bx011.haul.tripid, bx011.haul.vessel_id, bx011.haul.geartype, bx011.haul.fishingactivity_id, bx011.haul.end_ices_rectangle, bx011.haul.end_ices_area, bx011.haul.endlocation_latitude,bx011.haul.endlocation_longitude

                          FROM bx011.catch,
                               bx011.haul

                          WHERE bx011.catch.haul_id = bx011.haul.haul_id")

catch <- catch %>% filter(start_date >= DATE1, end_date <= DATE2) ## df containing full catch info for dates
haul.check.2022 <-haul%>%filter(start_date>=DATE1, end_date<=DATE2)
Vessel <- dbGetQuery(con, "SELECT bx011.vessel.vessel_name, bx011.vessel.vessel_rss, bx011.trip.tripid,bx011.trip.start_date, bx011.trip.end_date, bx011.trip.vessel_id


                          FROM bx011.vessel LEFT JOIN bx011.trip ON bx011.vessel.vessel_id = bx011.trip.vessel_id")

Vessel <- Vessel %>% filter(start_date >= DATE1, end_date <= DATE2) # extract right timeframe
names(Vessel)[names(Vessel) == "end_date"] <- "end_date_trip"
names(Vessel)[names(Vessel) == "start_date"] <- "start_date_trip"

#Check Vessel ID

unique(Vessel$vessel_id)

## identifying hauls with catch data

haul <- haul %>% filter(start_date >= DATE1, end_date <= DATE2)
hauls_identified <- length(unique(haul$haul_id))
hauls_identified
hauls_identified_with_catch_data <- length(unique(catch$haul_id))
hauls_identified_with_catch_data

# change the ices areas to match with WL.params2
catch$end_ices_area <- as.character(catch$end_ices_area)
catch$end_ices_area[catch$end_ices_area == "IVa"] <- "27.4.a"
catch$end_ices_area[catch$end_ices_area == "IVb"] <- "27.4.b"
catch$end_ices_area[catch$end_ices_area == "IVc"] <- "27.4.c"
catch$end_ices_area[catch$end_ices_area == "VIIa"] <- "27.7.a"
catch$end_ices_area[catch$end_ices_area == "VIIb"] <- "27.7.b"
catch$end_ices_area[catch$end_ices_area == "VIIc"] <- "27.7.c"
catch$end_ices_area[catch$end_ices_area == "VIId"] <- "27.7.d"
catch$end_ices_area[catch$end_ices_area == "VIIe"] <- "27.7.e"
catch$end_ices_area[catch$end_ices_area == "VIIf"] <- "27.7.f"
catch$end_ices_area[catch$end_ices_area == "VIIg"] <- "27.7.g"
catch$end_ices_area[catch$end_ices_area == "VIIh"] <- "27.7.h"
catch$end_ices_area[catch$end_ices_area == "VIIj"] <- "27.7.j"
catch$end_ices_area[catch$end_ices_area == "VIIk"] <- "27.7.k"
catch$end_ices_area[catch$end_ices_area == "VIa"] <- "27.6.a"
catch$end_ices_area[catch$end_ices_area == "VIb"] <- "27.6.b"
catch$end_ices_area[catch$end_ices_area == "VIc"] <- "27.6.c"
catch$end_ices_area[catch$end_ices_area == "VIIIA"] <- "27.8.a"
catch$end_ices_area[catch$end_ices_area == "VIIIB"] <- "27.8.b"
catch$end_ices_area[catch$end_ices_area == "VIIIC"] <- "27.8.c"
# Add Sex
catch$Sex <- "U" # could be updated with the tags column

# change the names of the variable to match with WL.params2
names(catch)[names(catch) == "species"] <- "Species"
names(catch)[names(catch) == "quarter"] <- "QUARTER"
names(catch)[names(catch) == "catchlength"] <- "Length"
names(catch)[names(catch) == "end_ices_area"] <- "ices_area"

## change the fate descriptions so we have consistent name between systems
unique(catch$fate)
catch$fate[catch$fate == "RET Gen"] <- "retainedGeneral"
catch$fate[catch$fate == "DIS Samp"] <- "discardedSample"
catch$fate[catch$fate == "RET Samp"] <- "retainedSample"
catch$fate[catch$fate == "DIS Gen"] <- "discardedGeneral"

catch <- catch[!(catch$fate == "NULL"), ] # delete lines with fate =NULL = 2 seabass that are in the skipper files but not seen on camera
catch <- catch[!(catch$fate == "Catch"), ] # delete fate=catch
unique(catch$fate)
## SubsampleCOD																		 

catch_cod <- catch %>%
  filter(Species == "COD")

# export csv to make changes (see emails)
#write.csv(catch_cod, file = "catch_cod.csv")

catch_cod <- read_csv("Z:\\C8505_Future_catch_policy\\Working_Area\\C8505C_Catch Data Monitoring (REM)\\Data Analysis\\Secondary\\COD Report Code\\catch_cod.csv")
## Add length weight conversions

d <- merge(catch_cod, WL.params2, all.x = T)
# head(d)
# d<-d %>% filter(year==2020 & QUARTER==4)#filter recent data
# convert length into 1cm interval
d$Length <- round(d$Length, 0)

# convert length to weight
d$Wt.At.Len_kg <- with(d, ((Length)^FACTOR.B) * FACTOR.A / 1000)

fish_dis_rf <- d %>%
  filter(fate %in% c("discardedGeneral")) %>%
  filter(is.na(weight)) %>%
  select(haul_id, numberfish) %>%
  rename(fish_rf = numberfish)


fish_ret_rf <- d %>%
  filter(fate %in% c("retainedGeneral")) %>%
  filter(is.na(weight)) %>%
  select(haul_id, numberfish) %>%
  rename(fish_rf = numberfish)


cod_general <- d %>%
  filter(fate %in% c("discardedGeneral", "retainedGeneral"))

cod_general <- d %>%
  filter(is.na(Wt.At.Len_kg)) %>%
  filter(weight > 0) # weights for hauls where fish were not measured


## haul weights

cod_sample <- d %>%
  filter(fate %in% c("retainedSample", "discardedSample"))

cod_sample_retained <- cod_sample %>%
  filter(fate == "retainedSample")

cod_sample_retained <- inner_join(cod_sample_retained, fish_ret_rf, by = "haul_id")

cod_sample_discarded <- cod_sample %>%
  filter(fate == "discardedSample")

cod_sample_discarded <- inner_join(cod_sample_discarded, fish_dis_rf, by = "haul_id")

cod_sample_ret_combined <- cod_sample_retained %>%
  group_by(haul_id) %>%
  mutate(total_weight = sum(Wt.At.Len_kg), no.fish = sum(numberfish)) %>%
  distinct(haul_id, .keep_all = TRUE) %>%
  mutate(sample_weight = total_weight * (fish_rf / no.fish))

cod_sample_dis_combined <- cod_sample_discarded %>%
  group_by(haul_id) %>%
  mutate(total_weight = sum(Wt.At.Len_kg), no.fish = sum(numberfish)) %>%
  distinct(haul_id, .keep_all = TRUE) %>%
  mutate(sample_weight = total_weight * (fish_rf / no.fish))

rec_dis <- sum(sum(cod_sample_dis_combined$sample_weight), sum(cod_gen_dis$general_weight))
rec_ret <- sum(sum(cod_sample_ret_combined$sample_weight), sum(cod_gen_ret$general_weight))

# Combining sample and gen weights.
cod_sample_ret_combined <- cod_sample_ret_combined %>%
  subset(select = -c(weight)) %>%
  add_column(general_weight = NA) %>%
  add_column(dis.ret = "retained")

cod_sample_dis_combined <- cod_sample_dis_combined %>%
  subset(select = -c(weight)) %>%
  add_column(general_weight = NA) %>%
  add_column(dis.ret = "discarded")

cod_gen_dis <- cod_general %>%
  filter(fate == "discardedGeneral") %>%
  rename(general_weight = weight) %>%
  add_column(sample_weight = NA) %>%
  add_column(dis.ret = "discarded")

cod_gen_ret <- cod_general %>%
  filter(fate == "retainedGeneral") %>%
  rename(general_weight = weight) %>%
  add_column(sample_weight = NA) %>%
  add_column(dis.ret = "retained")
# create rf to trip level
no.hauls <- haul %>%
  group_by(tripid) %>%
  mutate(total_hauls = length(unique(haul_id))) %>%
  select(tripid, total_hauls) %>%
  distinct(tripid, .keep_all = TRUE)

sampled_hauls <- catch %>%
  group_by(tripid) %>%
  mutate(sampled_hauls = length(unique(haul_id))) %>%
  select(tripid, sampled_hauls) %>%
  distinct(tripid, .keep_all = TRUE)

haul_rf <- inner_join(sampled_hauls, no.hauls, by = "tripid") %>%
  mutate(trip_rf = total_hauls / sampled_hauls)

# combine gen+sampled

ret_cod <- bind_rows(cod_gen_ret, cod_sample_ret_combined)
dis_cod <- bind_rows(cod_gen_dis, cod_sample_dis_combined)

ret_cod <- inner_join(haul_rf, ret_cod, by = "tripid") %>%
  mutate(recorded_weight = coalesce(general_weight, sample_weight)) %>%
  mutate(fate_weight = trip_rf * recorded_weight) %>%
  group_by(tripid) %>%
  mutate(trip_weight = sum(fate_weight)) %>%
  distinct(tripid, .keep_all = TRUE)


dis_cod <- inner_join(haul_rf, dis_cod, by = "tripid") %>%
  mutate(recorded_weight = coalesce(general_weight, sample_weight)) %>%
  mutate(fate_weight = trip_rf * recorded_weight) %>%
  group_by(tripid) %>%
  mutate(trip_weight = sum(fate_weight)) %>%
  distinct(tripid, .keep_all = TRUE)

## values used in cod 2 pager

cod_retained_weight <- sum(ret_cod$trip_weight)
cod_discarded_weight <- sum(dis_cod$trip_weight)

#
analysed_trips <- n_distinct(catch$tripid)
analysed_hauls <- n_distinct(catch$haul_id)
# recorded weight is the sample weight and the general weights added together
# fate weight is recorded weight for each haul, raised to trip level
# trip weight is the raised haul weights added together

# raise to vessel level

no.trips <- haul %>%
  group_by(vessel_id) %>%
  mutate(total_trips = length(unique(tripid))) %>%
  select(vessel_id, total_trips) %>%
  distinct(vessel_id, .keep_all = TRUE)

sampled_trips <- catch %>%
  group_by(vessel_id) %>%
  mutate(sampled_trips = length(unique(tripid))) %>%
  select(vessel_id, sampled_trips) %>%
  distinct(vessel_id, .keep_all = TRUE)

vessel_rf <- inner_join(no.trips, sampled_trips, by = "vessel_id") %>%
  mutate(vessel_rf = total_trips / sampled_trips)

ret_cod_vessel <- inner_join(vessel_rf, ret_cod, by = "vessel_id") %>%
  mutate(vessel_weight = trip_weight * vessel_rf) %>%
  group_by(vessel_id) %>%
  mutate(vessel_weight_raised = sum(vessel_weight)) %>%
  distinct(vessel_id, .keep_all = TRUE)

#
dis_cod_vessel <- inner_join(vessel_rf, dis_cod, by = "vessel_id") %>%
  mutate(vessel_weight = trip_weight * vessel_rf) %>%
  group_by(vessel_id) %>%
  mutate(vessel_weight_raised = sum(vessel_weight)) %>%
  distinct(vessel_id, .keep_all = TRUE)

# }

# cod ret vs dis count
cod_ret_count <- catch_cod %>%
  filter(fate %in% c("retainedGeneral"))

cod_ret_count <- sum(cod_ret_count$numberfish)

cod_dis_count <- catch_cod %>%
  filter(fate %in% c("discardedGeneral"))

cod_dis_count <- sum(cod_dis_count$numberfish)

## 2021 checks

ret_cod <- inner_join(haul_rf, ret_cod, by = "tripid") %>%
  mutate(recorded_weight = coalesce(general_weight, sample_weight)) %>%
  group_by(vessel_id) %>%
  mutate(recorded_vessel_weight = sum(recorded_weight)) %>%
  distinct(vessel_id, .keep_all = TRUE)

dis_cod <- inner_join(haul_rf, dis_cod, by = "tripid") %>%
  mutate(recorded_weight = coalesce(general_weight, sample_weight)) %>%
  group_by(vessel_id) %>%
  mutate(recorded_vessel_weight = sum(recorded_weight)) %>%
  distinct(vessel_id, .keep_all = TRUE)

cod_counts <- cod_general %>%
  group_by(vessel_id) %>%
  mutate(number_counted = sum(numberfish)) %>%
  distinct(vessel_id, .keep_all = TRUE)
## Plotting

cod_sample <- cod_sample %>%
  add_column(dis.ret = NA)

cod_sample$dis.ret[cod_sample$fate == "retainedSample"] <- "Retained "
cod_sample$dis.ret[cod_sample$fate == "discardedSample"] <- "Discarded"

cod_sample <- data.frame(cod_sample[rep(seq_len(dim(cod_sample)[1]), cod_sample$numberfish), , drop = FALSE], row.names = NULL)

library(ggplot2)
ggplot(data = cod_sample, aes(x = Length, fill = dis.ret)) +
  geom_histogram(boundary = 0, binwidth = 5, colour = "black") +
  scale_x_continuous(breaks = seq(0, 100, by = 5), name = "Length (cm)") +
  scale_y_continuous(breaks = seq(0, 90, by = 5), name = "Number at Length") +
  theme(panel.grid.minor = element_blank()) +
  scale_fill_manual(name = "Fate", values = c("#006E8C", "#00AAB4")) +
  geom_vline(
    xintercept = 35, linetype = "dashed",
    color = "red", size = 1
  )
