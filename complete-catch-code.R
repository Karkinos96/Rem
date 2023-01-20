###REM Catch Data Analysis

##Required Packages ----

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

##Importing REM Data ----

con <- dbConnect(
  Postgres(),
  host = "citpgsqlbx011.postgres.database.azure.com",
  dbname = "bx011",
  port = 5432,
  user = "ana_rs@citpgsqlbx011",
  password = "FQGg_v++5=T9"
)

dbListTables(con)

#exploring db
expl_catch <- dbGetQuery(con, "SELECT  * FROM bx011.catch") # raw unsorted catch data

haul <- dbGetQuery(con, "SELECT  * FROM bx011.haul") # haul information

expl_trip <- dbGetQuery(con, "SELECT  * FROM bx011.trip") # trip information

expl_vessel <- dbGetQuery(con, "SELECT  * FROM bx011.vessel") # list of all vessels around the UK

species <- dbGetQuery(con, "SELECT  * FROM bx011.species") # list of cefas vessel species codes

#extract data for required dates

DATE1 <- "2022-01-01" ## start date of data range
DATE2 <- "2022-12-31" ## end date of data range

catch <- dbGetQuery(con, "SELECT  bx011.catch.catch_id, bx011.catch.haul_id, bx011.haul.haul_number,  date_part('quarter', bx011.haul.end_date) as QUARTER,  date_part('month', bx011.haul.end_date) as MONTH,
                                 bx011.haul.end_year, bx011.haul.start_date, bx011.haul.start_time,bx011.haul.end_date, bx011.haul.end_time, bx011.catch.species as Species, bx011.catch.species_comment, bx011.catch.tags,bx011.catch.state, bx011.catch.fate, bx011.catch.numberfish,
                                 bx011.catch.catchlength, bx011.catch.weight, bx011.catch.volume, bx011.catch.volume_units,
                                 bx011.haul.tripid, bx011.haul.vessel_id, bx011.haul.geartype, bx011.haul.fishingactivity_id, bx011.haul.end_ices_rectangle, bx011.haul.end_ices_area, bx011.haul.endlocation_latitude,bx011.haul.endlocation_longitude

                          FROM bx011.catch,
                               bx011.haul

                          WHERE bx011.catch.haul_id = bx011.haul.haul_id") %>%
  filter(start_date >= DATE1, end_date <= DATE2)

unique(catch$vessel_id)#check for correct PLN

##Generating effort & RF ----

#hauls made, hauls sampled

haul <- haul %>% filter(start_date >= DATE1, end_date <= DATE2)
hauls_identified <- length(unique(haul$haul_id))
hauls_identified
hauls_identified_with_catch_data <- length(unique(catch$haul_id))
hauls_identified_with_catch_data

#trips made, trips sampled
trip<-trip%>%
  mutate(pln=case_when(vessel_id==254417~'FY221',
                       vessel_id==256143~'SS118'))
trip <- expl_trip %>% filter(start_date >= DATE1, end_date <= DATE2)
trips_identified <- length(unique(trip$tripid))
trips_identified
trips_identified_with_catch_data <- length(unique(catch$tripid))
trips_identified_with_catch_data

##Corrections----

#Record any corrections here and if they have been uploaded to the DB

