rm(list=(ls()))

#setwd("Z:\\BX011_WP8_ManRegimes\\Working_Area\\2020-21\\BX011M Database\\Test_Database_ARS")
setwd("Z:\\BX011_WP8_ManRegimes\\Working_Area\\BX011R - Continous Monitoring REM data\\MD")

install.packages("RPostgreSQL")
install.packages("RPostgres")
install.packages("tidyverse")
install.packages("mappplots")
install.packages("odbc")

require (RPostgreSQL); 
library(DBI)
library (RODBC)
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


con <- dbConnect(
  Postgres(),
  host = "citpgsqlbx011.postgres.database.azure.com",
  dbname = "bx011",
  port = 5432,
  user = "ana_rs@citpgsqlbx011",
  password = "FQGg_v++5=T9")

dbListTables(con)

##get the length weight conversion factors
WL.params2 <- read.table("Ref_tables\\WL.params.2014.txt", sep="\t", header=T)

#
#change the ices areas names
{WL.params2$ices_area<- as.character(WL.params2$Region)
  WL.params2$ices_area[WL.params2$ices_area=="101A"] <- "27.1.a"
  WL.params2$ices_area[WL.params2$ices_area=="101D"] <- "27.1.d"
  WL.params2$ices_area[WL.params2$ices_area=="101G"] <- "27.1.g"
  WL.params2$ices_area[WL.params2$ices_area=="102A"] <- "27.2.a"
  WL.params2$ices_area[WL.params2$ices_area=="102B"] <- "27.2.b"
  WL.params2$ices_area[WL.params2$ices_area=="102C"] <- "27.2.c"
  WL.params2$ices_area[WL.params2$ices_area=="102D"] <- "27.2.d"
  WL.params2$ices_area[WL.params2$ices_area=="102E"] <- "27.2.e"
  WL.params2$ices_area[WL.params2$ices_area=="102F"] <- "27.2.f"
  WL.params2$ices_area[WL.params2$ices_area=="102G"] <- "27.2.g"
  WL.params2$ices_area[WL.params2$ices_area=="103A"] <- "27.3.a"
  WL.params2$ices_area[WL.params2$ices_area=="104A"] <- "27.4.a"
  WL.params2$ices_area[WL.params2$ices_area=="104B"] <- "27.4.b"
  WL.params2$ices_area[WL.params2$ices_area=="104C"] <- "27.4.c"
  WL.params2$ices_area[WL.params2$ices_area=="107A"] <- "27.7.a"
  WL.params2$ices_area[WL.params2$ices_area=="107B"] <- "27.7.b"
  WL.params2$ices_area[WL.params2$ices_area=="107C"] <- "27.7.c"
  WL.params2$ices_area[WL.params2$ices_area=="107D"] <- "27.7.d"
  WL.params2$ices_area[WL.params2$ices_area=="107E"] <- "27.7.e"
  WL.params2$ices_area[WL.params2$ices_area=="107F"] <- "27.7.f"
  WL.params2$ices_area[WL.params2$ices_area=="107G"] <- "27.7.g"
  WL.params2$ices_area[WL.params2$ices_area=="107H"] <- "27.7.h"
  WL.params2$ices_area[WL.params2$ices_area=="107i"] <- "27.7.i"
  WL.params2$ices_area[WL.params2$ices_area=="107J"] <- "27.7.j"
  WL.params2$ices_area[WL.params2$ices_area=="107K"] <- "27.7.k"
  WL.params2$ices_area[WL.params2$ices_area=="106A"] <- "27.6.a"
  WL.params2$ices_area[WL.params2$ices_area=="106B"] <- "27.6.b"
  WL.params2$ices_area[WL.params2$ices_area=="106C"] <- "27.6.c"
  WL.params2$ices_area[WL.params2$ices_area=="108A"] <- "27.8.a"
  WL.params2$ices_area[WL.params2$ices_area=="108B"] <- "27.8.b"
  WL.params2$ices_area[WL.params2$ices_area=="108C"] <- "27.8.c"
  WL.params2$ices_area[WL.params2$ices_area=="109A"] <- "27.9.a"
  WL.params2$ices_area[WL.params2$ices_area=="109B"] <- "27.9.b"
}
head(WL.params2)
###
#get the catch data from PGadmin
#explore tables
expl_catch <- dbGetQuery(con, "SELECT  * FROM bx011.catch")

summary(expl_catch)# nothing in catch.time, catch.location_lat, location_long, point accuracy, fishing activity, created_time... what is state?

haul<-dbGetQuery(con, "SELECT  * FROM bx011.haul")
unique(haul$vessel_id)
#haul$vessel_id [haul$vessel_id %in% c ("FY221")] <- "254417"
#haul$vessel_id [haul$vessel_id %in% c ("SS118")] <- "256143"
#haul$vessel_id [haul$vessel_id %in% c ("PZ329")] <- "257134"
#haul$vessel_id [haul$vessel_id %in% c ("CK925")] <- "259725"
#haul$vessel_id [haul$vessel_id %in% c ("FH145")] <- "238033"
#haul$tripid [haul$haul_id %in% c ("a185646", "a185647","a185648")] <- "t5592"

expl_trip<-dbGetQuery(con, "SELECT  * FROM bx011.trip")
unique(expl_trip$vessel_id)


expl_vessel<-dbGetQuery(con, "SELECT  * FROM bx011.vessel")
summary(expl_vessel)

species<-dbGetQuery(con, "SELECT  * FROM bx011.species")

#select relevant columns:
#catch <- dbGetQuery(con, "SELECT  bx011.catch.catch_id, bx011.catch.haul_id, bx011.catch.tags, date_part('quarter', bx011.catch.date) as QUARTER,  
#                                bx011.catch.year, bx011.catch.date, bx011.catch.species as Species, bx011.catch.species_comment, bx011.catch.fate, bx011.catch.numberfish, 
#                               bx011.catch.catchlength, bx011.catch.weight, bx011.catch.volume, bx011.catch.volume_units,bx011.catch.created_by,bx011.catch.tags,
#                              bx011.haul.tripid, bx011.haul.geartype,bx011.haul.fishingactivity_id, bx011.haul.start_ices_rectangle, bx011.haul.start_ices_area, bx011.haul.startlocation_latitude,bx011.haul.startlocation_longitude
#
#                         FROM bx011.catch,
#                             bx011.haul
#             
#                      WHERE bx011.catch.haul_id = bx011.haul.haul_id")

#date in catch table is related to the time entered, so date from the haul table should be added!
#no haul_type needed for non selectivity data: delete or add bx011.haul.haul_type,
#changed haul.start_locations/rectangles... to haul.end_locations/rectangles/... 
# need to add skipper ID, haul number to connect with skipper data


#time frame selection from DATE1 till DATE2
DATE1<-"2022-01-01"
DATE2<-"2022-12-31"

#start extraction: added haul_number, month, state on March 2022
catch <- dbGetQuery(con, "SELECT  bx011.catch.catch_id, bx011.catch.haul_id, bx011.haul.haul_number,  date_part('quarter', bx011.haul.end_date) as QUARTER,  date_part('month', bx011.haul.end_date) as MONTH,
                                 bx011.haul.end_year, bx011.haul.start_date, bx011.haul.start_time,bx011.haul.end_date, bx011.haul.end_time, bx011.catch.species as Species, bx011.catch.species_comment, bx011.catch.tags,bx011.catch.state, bx011.catch.fate, bx011.catch.numberfish, 
                                 bx011.catch.catchlength, bx011.catch.weight, bx011.catch.volume, bx011.catch.volume_units,
                                 bx011.haul.tripid, bx011.haul.vessel_id, bx011.haul.geartype, bx011.haul.fishingactivity_id, bx011.haul.end_ices_rectangle, bx011.haul.end_ices_area, bx011.haul.endlocation_latitude,bx011.haul.endlocation_longitude

                          FROM bx011.catch,
                               bx011.haul
                
                          WHERE bx011.catch.haul_id = bx011.haul.haul_id")


catch<-catch%>% filter(start_date>= DATE1, end_date<= DATE2)#extract right time frame
haul.check.2022<-haul%>% filter(start_date>= DATE1, end_date<= DATE2)

unique(catch$vessel_id)#some have PLN number instead of vessel id
#corrections
#catch$vessel_id [catch$vessel_id %in% c ("FY221")] <- "254417"
#catch$vessel_id [catch$vessel_id %in% c ("SS118")] <- "256143"
#catch$vessel_id [catch$vessel_id %in% c ("PZ329")] <- "257134"
#catch$vessel_id [catch$vessel_id %in% c ("FH145")] <- "238033"
#catch$vessel_id [catch$vessel_id %in% c ("CK925")] <- "259725"
#unique(catch$vessel_id)
#catch$vessel_id [catch$tripid %in% c ("t5650")] <- "259725"#wrong vessel ID should be Venture

#changed start_date into end_date
#added vessel_rss, renamed trip end date to not mess it up with the haul end date when merging later on

Vessel <- dbGetQuery(con, "SELECT bx011.vessel.vessel_name, bx011.vessel.vessel_rss, bx011.trip.tripid,bx011.trip.start_date, bx011.trip.end_date, bx011.trip.vessel_id
                                

                          FROM bx011.vessel LEFT JOIN bx011.trip ON bx011.vessel.vessel_id = bx011.trip.vessel_id")


Vessel<-Vessel%>% filter(start_date>= DATE1, end_date<= DATE2)#extract right timeframe
names(Vessel)[names(Vessel) == "end_date"] <- "end_date_trip"
names(Vessel)[names(Vessel) == "start_date"] <- "start_date_trip"
unique(Vessel$vessel_id)
#Vessel$vessel_id [Vessel$vessel_id %in% c ("264183")] <- "257134"#wrong vessel ID
#Vessel$vessel_id [Vessel$vessel_id %in% c ("264017")] <- "259725"#wrong vessel ID

unique(Vessel$vessel_id)
unique(Vessel$vessel_name)
unique(Vessel$vessel_rss)

haul<-haul%>% filter(start_date>= DATE1, end_date<= DATE2)
hauls_identified<-length(unique(haul$haul_id))
hauls_identified
hauls_identified_with_catch_data<-length(unique(catch$haul_id))
hauls_identified_with_catch_data 


#change the ices areas to match with WL.params2
{catch$end_ices_area<- as.character(catch$end_ices_area)
  catch$end_ices_area[catch$end_ices_area=="IVa"] <- "27.4.a"
  catch$end_ices_area[catch$end_ices_area=="IVb"] <- "27.4.b"
  catch$end_ices_area[catch$end_ices_area=="IVc"] <- "27.4.c"
  catch$end_ices_area[catch$end_ices_area=="VIIa"] <- "27.7.a"
  catch$end_ices_area[catch$end_ices_area=="VIIb"] <- "27.7.b"
  catch$end_ices_area[catch$end_ices_area=="VIIc"] <- "27.7.c"
  catch$end_ices_area[catch$end_ices_area=="VIId"] <- "27.7.d"
  catch$end_ices_area[catch$end_ices_area=="VIIe"] <- "27.7.e"
  catch$end_ices_area[catch$end_ices_area=="VIIf"] <- "27.7.f"
  catch$end_ices_area[catch$end_ices_area=="VIIg"] <- "27.7.g"
  catch$end_ices_area[catch$end_ices_area=="VIIh"] <- "27.7.h"
  catch$end_ices_area[catch$end_ices_area=="VIIj"] <- "27.7.j"
  catch$end_ices_area[catch$end_ices_area=="VIIk"] <- "27.7.k"
  catch$end_ices_area[catch$end_ices_area=="VIa"] <- "27.6.a"
  catch$end_ices_area[catch$end_ices_area=="VIb"] <- "27.6.b"
  catch$end_ices_area[catch$end_ices_area=="VIc"] <- "27.6.c"
  catch$end_ices_area[catch$end_ices_area=="VIIIA"] <- "27.8.a"
  catch$end_ices_area[catch$end_ices_area=="VIIIB"] <- "27.8.b"
  catch$end_ices_area[catch$end_ices_area=="VIIIC"] <- "27.8.c"
}

# Add Sex
catch$Sex <- "U"#could be updated with the tags column

# change the names of the variable to match with WL.params2
names(catch)[names(catch)=="species" ]<- "Species"
names(catch)[names(catch)=="quarter" ]<- "QUARTER"
names(catch)[names(catch)=="catchlength" ]<- "Length"
names(catch)[names(catch)=="end_ices_area" ]<- "ices_area"

## change the fate descriptions so we have consistent name between systems
unique(catch$fate)
catch$fate[catch$fate =="RET Gen"] <- "retainedGeneral"
catch$fate[catch$fate =="DIS Samp"] <- "discardedSample"
catch$fate[catch$fate =="RET Samp"] <- "retainedSample"
catch$fate[catch$fate =="DIS Gen"] <- "discardedGeneral"

catch<-catch[ !(catch$fate=="NULL"),]#delete lines with fate =NULL = 2 seabass that are in the skipper files but not seen on camera
catch<-catch[ !(catch$fate=="Catch"),]#delete fate=catch
unique(catch$fate)

#########################################
###1. trips and hauls fished / sampled###
#########################################
#start changed to end
trip <- dbGetQuery(con, "SELECT bx011.haul.tripid, bx011.haul.haul_id, bx011.haul.geartype,
                                bx011.haul.end_ices_rectangle, 
                                bx011.gear.meshsize, bx011.gear.netlength

                          FROM bx011.haul LEFT JOIN bx011.gear ON bx011.gear.haul_id = bx011.haul.haul_id")

trip$tripid [trip$haul_id %in% c ("a185646", "a185647","a185648")] <- "t5592"
TRIP<-merge(trip, Vessel, all.x = TRUE)
TRIP<-TRIP%>% filter(start_date_trip>= DATE1, end_date_trip< DATE2)
unique(TRIP$vessel_id)
unique(TRIP$vessel_name)
unique(haul$vessel_id)
unique(catch$vessel_id)


#TRIP<-filter(TRIP, TRIP$vessel_name %in% c("COPIOUS","VENTURE"))#select the vessel that did selectivity work
#TRIP<-filter(TRIP, TRIP$vessel_id %in% c("238033","259725")) # select vessel for selectivity work based on vessel ID as there are NA in vessel name
Vessel_trips<-TRIP %>%
  group_by(vessel_name, vessel_id)%>% 
  summarize(.,No.hauls.fished = n_distinct(haul_id),No.trips.fished = n_distinct(tripid) ) # do we need the name of the vessels or rather id?


#Vessel_trips_haul_type<-TRIP %>%
#  group_by(vessel_id, haul_type)%>% summarize(.,No.hauls.fished = n_distinct(haul_id),No.trips.fished = n_distinct(tripid) )

catch<-merge(catch,Vessel,all.x=TRUE)
#CATCH<-CATCH%>% filter(start_date>= "2020-10-01")#All before October 2020 is dummy data

unique(catch$vessel_name)
unique(catch$vessel_id)#corrected end date of vessel is end date of the trip while end date in the catch table is end date of the haul, 

#catch<-filter(catch, vessel_id %in% c ("238033","259725"))
#catch<-filter(catch, vessel_name %in% c ("COPIOUS","VENTURE"))

Vessel_catch<-catch%>% 
  group_by(vessel_name, vessel_id)%>% summarize(.,No.hauls.sampled = n_distinct(haul_id),No.trips.sampled = n_distinct(tripid) ) 

#Vessel_catch_haul_type<-catch%>% 
# group_by(vessel_id, haul_type)%>% summarize(.,No.hauls.sampled = n_distinct(haul_id),No.trips.sampled = n_distinct(tripid) ) 

Vessel_fished_sampled<-merge(Vessel_trips,Vessel_catch, all.x=TRUE)
#Vessel_fished_sampled_haul_type<-merge(Vessel_trips_haul_type,Vessel_catch_haul_type, all.x=TRUE)


#add totals line
Vessel_fished_sampled<-Vessel_fished_sampled %>% bind_rows (summarise_all(., ~if(is.numeric(.)) sum(.) else "Total"))
#Vessel_fished_sampled_haul_type<-Vessel_fished_sampled_haul_type %>% bind_rows (summarise_all(., ~if(is.numeric(.)) sum(.) else "Total"))


write.csv(Vessel_fished_sampled, "./tables/Vessel_fished_sampled_sarah_16_03_2022.csv")
#write.csv(Vessel_fished_sampled_haul_type, "Vessel_fished_sampled_haul_type_28_09_2021.csv")

####################################
###2. fishing activity: locations###
####################################
#changed start to end locations
# added trip start_date, there are a lot of trips without haul id, need to figure ou what date these trips came from and if they are still relevant
location <- dbGetQuery(con, "SELECT bx011.trip.tripid, bx011.trip.vessel_id, bx011.haul.start_date,bx011.haul.haul_id,
                                bx011.haul.endlocation_latitude, bx011.haul.endlocation_longitude,bx011.haul.end_date, bx011.haul.end_time, bx011.haul.end_ices_rectangle, bx011.haul.end_ices_area
                                

                          FROM bx011.trip LEFT JOIN bx011.haul ON bx011.trip.tripid = bx011.haul.tripid")
unique(location$vessel_id)
location$vessel_id [location$vessel_id %in% c ("264183")] <- "257134"#wrong vessel ID
location$vessel_id [location$vessel_id %in% c ("264017")] <- "259725"#wrong vessel ID
location<-location%>% filter(start_date>= DATE1, end_date<= DATE2)
error<-location[is.na(location$haul_id),]
#write.csv(error, "Z:\\BX011_WP8_ManRegimes\\Working_Area\\2021-22\\BX011V - Data Representativeness\\data and scripts\\MD\\NA_haul_id.csv")
location<-location[!is.na(location$haul_id),]# remove trips with NA after double check from Becca
location$tripid [location$haul_id %in% c ("a185646", "a185647","a185648")] <- "t5592"

locations<-merge(location,Vessel, all.x = TRUE)
unique(locations$vessel_name)
unique(locations$vessel_id)#
# take the unique haul id of the catch table and add to the locations data to indicate what hauls contain catch data

catch_locations<-unique(catch$haul_id)
locations$catch_data[locations$haul_id %in% catch_locations]<-TRUE
length(which(locations$catch_data == TRUE))

write.csv(locations,"./tables/locations1_cont_16_03_2022.csv")


#effort table:
effort<-merge(catch,TRIP[, c("haul_id","meshsize")], by="haul_id", all.x=TRUE )
effort<-effort%>%
  group_by(end_year, month, QUARTER, geartype, ices_area, end_ices_rectangle)%>%
  summarize(.,No.hauls.sampled = n_distinct(haul_id),No.trips.sampled = n_distinct(tripid) )

write.csv(effort,"./tables/effort_cont_16_03_2022.csv")

#raise to trip
RF_trip1<-catch%>% 
  group_by(tripid,vessel_name, vessel_id)%>% summarize(.,No.hauls.sampled = n_distinct(haul_id),No.trips.sampled = n_distinct(tripid) ) 

locations2<-locations
locations2$catch_data[is.na(locations2$catch_data)] <- 0
RD_trip<-locations2%>% 
  group_by(vessel_name, vessel_id, tripid)%>% summarize(.,No.hauls.fished = n_distinct(haul_id),No.hauls.sampled = n_distinct(haul_id[catch_data==TRUE]) ) 
RD_trip$RF_trip<-RD_trip$No.hauls.fished / RD_trip$No.hauls.sampled
locations2<-merge(locations2,RD_trip)

#locations<-merge(locations, RD_trip[, c("tripid","RD_trip")], by="tripid", all.x=TRUE)
#raise to vessel
RD_Vessel<-locations2%>% 
  group_by(vessel_name, vessel_id)%>% summarize(.,No.hauls.fished.vessel = n_distinct(haul_id),No.trips.fished.vessel = n_distinct(tripid),No.trips.sampled.vessel = n_distinct(tripid[catch_data==TRUE]), No.hauls.sampled.vessel = n_distinct(haul_id[catch_data==TRUE])  ) 
RD_Vessel$RF_Vessel<-RD_Vessel$No.trips.fished.vessel / RD_Vessel$No.trips.sampled.vessel
locations2<-merge(locations2, RD_Vessel)

locations2<-locations2%>% filter(catch_data== 1)# only keep where there is catch data to raise with the raising factors, needs merging later on

write.csv(locations2,"./tables/locations2_cont_16_03_2022.csv")
#Add Gear type and mesh size

locations3<-merge(locations2, TRIP[, c("haul_id","geartype", "meshsize")], by="haul_id", all.x=TRUE)
#write.csv(locations3,"Z:\\BX011_WP8_ManRegimes\\Working_Area\\2021-22\\BX011V - Data Representativeness\\data and scripts\\MD\\locations2_28_02_2022.csv")

# plot lat and longs
######################

#some hauls are not in... haul id... t3657, t3659, t3885, t3973, t3974 and t4425 no lat long data 
library(mapdata)
library(mapplots)
library(rworldmap)
library(vmstools)
library(cowplot)

#data(ICESareas) #to print ICEs rectangle on the plot
#model <- c("27.7.b", "27.7.c", "27.7.c.1", "27.7.c.2", "27.7.e", "27.7.f", "27.7.g", "27.7.h", "27.7.j", "27.7.j.1", "27.7.j.2", "27.7.k", "27.7.k.1", "27.7.k.2")

#setup the lat/lon limits of the plot
xlim <- c(-8,0)
ylim <- c(48,52)

#get the coastline
coast <- map_data("worldHires", xlim = xlim, ylim=ylim)
coast.poly <- geom_polygon(data=coast, aes(x=long, y=lat, group=group), colour= "#999999", fill="#999999", lwd=0.2)
coast.outline <- geom_path(data=coast, aes(x=long, y=lat, group=group), colour= "#555555", lwd=0.2)

head(locations)
plot <- 
  ggplot()+
  #facet_grid()
  geom_polygon(data=coast, aes(x=long, y=lat, group=group), colour= "#999999", fill="#999999", lwd=0.2)+
  geom_point(data=locations ,aes(x=startlocation_longitude, y =startlocation_latitude, color=vessel_name)) +
  #geom_tile(data=land4,aes(x=LonMid,y=LatMid,fill=Landings),linetype=2)+
  #geom_tile(data=land4,aes(x=LonMid,y=LatMid,fill=Weight),linetype=2)+
  geom_path(data=coast, aes(x=long, y=lat, group=group), colour= "#555555", lwd=0.2)+
  coord_quickmap(xlim,ylim)+
  #facet_wrap( ~ year )+
  #geom_point(data=DATA2 ,aes(x=LonMid, y =LatMid, lwd=No_obs_trips))+
  #scale_fill_gradient(low="yellow", high="red",na.value="paleturquoise")+
  
  #geom_polygon(data=coast, aes(x=long, y=lat, group=group), colour= "green4", fill="green4", lwd=0.2)+
  geom_hline(yintercept=seq(47,60,by=0.5),colour="grey")+
  geom_vline(xintercept=seq(-12,10,by=1),colour="grey")+
  #geom_polygon(aes(x=long,y=lat,group=group), data = ICESareas[ICESareas$Area_Full %in%  c("27.7.f","27.7.e","27.7.g","27.7.h","27.7.j", "27.7.j.1","27.7.j.2"),]) +
  #geom_polygon(aes(x=long,y=lat,group=group),size=0.5,colour="black",data=ICESareas,alpha=0) + #with ICES rectangles printed
  #annotate("text",x=-3.5,y=49.5,label="7e") + annotate("text",x=-8,y=49,label="7h") + #text on ICES rectangles
  #annotate("text",x=-11,y=51,label="7j") + annotate("text",x=-5.5,y=50.8,label="7f") + # text on ICEs rectangles
  #annotate("text",x=-8,y=51,label="7g") + #geom_polygon(data=coast, aes(x=long, y=lat, group=group), colour= "green4", fill="green4", lwd=0.2)+
  #geom_path(data=coast, aes(x=long, y=lat, group=group), colour= "green4", lwd=0.2)+
  #geom_segment(data=DATA2,aes(x=2.5,xend=2.5,y=48.8,yend=49.5),arrow=arrow(length=unit(0.5,"cm")))+
  #geom_path(data=NArrow,aes(x=X,y=Y),linejoin="mitre",linetype=1)+
  labs(x = "Longitude",y="Latitude")+
  #ggtitle (vessels[i]) +
  theme (legend.position = "bottom")+
  theme(plot.margin=grid::unit(c(0,0,0,0), "mm"))+
  guides(fill = guide_colourbar(barwidth = 10, barheight = 1))
#(filename= paste ("outputs\\", paste(vessels[i], "PLE_by_rect_vessel.jpeg",  sep="_")), width = 50, height = 30, units = "cm")
#ggsave(filename= paste ("data\\FSP Monkfish survey\\", paste("try3", "Weight_monkfish.jpeg",  sep="_")), width = 50, height = 20, units = "cm")



##############################################
###3. Calculate the raising factor for haul###
##############################################

#1. calculate the weight caught in each haul, retained and discards
# add Catch component. Need to make sure that we cover all fates

catch$catch_comp <- "Retained"
catch$catch_comp [catch$fate %in% c ("discardedSample", "discardedGeneral", "discardedDamaged")] <- "Discards"
catch$catch_comp [catch$fate %in% c ("Catch")] <- "Total_catch"

#CORRECTIONS
catch$Length[catch$Species=="MON" & catch$Length == 369.85]<-37# previous non selective
catch$Length[catch$Species=="HKE" & catch$haul_id == c("a146308") & catch$Length == "3.5"]<-NA# check again when vessel Venture is in
catch$Weight[catch$Species=="HKE" & catch$haul_id == c("a146308") ]<-3.5

catch$volume[catch$Species=="WHG" & catch$haul_id == c("a144686") & catch$volume == "1.2"]<-NA
catch$weight[catch$Species=="WHG" & catch$haul_id == c("a144686") ]<-1.2
catch$Species[catch$Species=="HAD" & catch$haul_id == c("a144686") & catch$fate== "discardedSample" & catch$Length == "30.55"]<-"WHG"
catch$numberfish[catch$Species=="HAD" & catch$haul_id == c("a144686") & catch$fate== "discardedGeneral"]<-3

catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a144618") & catch$fate== "discardedGeneral"]<-7.6
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a144626") & catch$fate== "discardedGeneral"]<-16
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a144629") & catch$fate== "discardedGeneral"]<-6.4
catch$weight[catch$Species=="BEN" & catch$haul_id == c("a144678") & catch$fate== "discardedGeneral"]<-5.5
catch$weight[catch$Species=="LSD" & catch$haul_id == c("a144678") & catch$fate== "discardedGeneral"]<-125
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a144678") & catch$fate== "discardedGeneral"]<-7.6
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a144687") & catch$fate== "discardedGeneral"]<-7.1
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a146323") & catch$fate== "discardedGeneral"]<-6.5
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a146466") & catch$fate== "discardedGeneral"]<-6
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a157665") & catch$fate== "discardedGeneral"]<-6
catch$weight[catch$Species=="DGS" & catch$haul_id == c("a157684") & catch$fate== "discardedGeneral"]<-1.5
catch$weight[catch$Species=="ZZZ" & catch$haul_id == c("a158112") & catch$fate== "discardedGeneral"]<-10.5
catch$weight[catch$Species=="LSD" & catch$haul_id == c("a158114") & catch$fate== "discardedGeneral"]<-90
catch$weight[catch$Species=="ANF" & catch$haul_id == c("a165290") & catch$fate== "discardedGeneral"]<-0.2
catch$weight[catch$Species=="COD" & catch$haul_id == c("a157675") & catch$fate== "retainedGeneral"]<-0.5
catch$weight[catch$Species=="WHG" & catch$haul_id == c("a157675") & catch$fate== "retainedGeneral"]<-0.3
catch$weight[catch$Species=="CUR" & catch$haul_id == c("a158114") & catch$fate== "discardedGeneral"]<-1.3

# lengths in discarded general
catch$fate[catch$Species=="LEM" & catch$haul_id == c("a157674") & catch$fate== "discardedGeneral" & catch$Length>1 ]<-"discardedSample"
catch$fate[catch$Species=="HAD" & catch$haul_id == c("a157674") & catch$fate== "discardedGeneral" & catch$Length>1 ]<-"discardedSample"

#lengths in retained general
catch$fate[catch$Species=="MON" & catch$haul_id == c("a103025") & catch$fate== "retainedGeneral" & catch$Length>1 ]<-"retainedSample"
catch$numberfish[catch$Species=="HAD" & catch$haul_id == c("a148703")& catch$fate== "retainedGeneral"]<-8
catch$Length[catch$Species=="HAD" & catch$haul_id == c("a148703")& catch$fate== "retainedGeneral"]<-NA
catch$fate[catch$Species=="ESB" & catch$haul_id == c("a196497") & catch$fate== "retainedGeneral" & catch$Length>1 ]<-"retainedSample"
#08/03/2022
catch<-catch[ !(catch$Species=="ANF" & catch$haul_id=="a91939"),]#there are measured MON/WAF - ANF is the total
catch<-catch[ !(catch$Species=="ANF" & catch$haul_id=="a91944"),]
catch$weight[catch$Species=="HKE" & catch$haul_id == c("a146308") & catch$fate== "retainedGeneral"]<-3.5
catch$weight[catch$Species=="MUR" & catch$haul_id == c("a146308") & catch$fate== "retainedGeneral"]<-1.25
catch$number[catch$Species=="MUR" & catch$haul_id == c("a146308") & catch$fate== "retainedGeneral"]<-7
catch$weight[catch$Species=="SQC" & catch$haul_id == c("a146307") & catch$fate== "retainedGeneral"]<-0.5
catch$weight[catch$Species=="WHG" & catch$haul_id == c("a146304") & catch$fate== "retainedGeneral"]<-0.4


# check species comments for MIX, PRI, ZZZ
head(catch)
catch_comments<-select(catch,tripid, vessel_name, haul_id, haul_number, end_date,end_time, Species, species_comment, tags, state, catch_comp, fate, numberfish, weight, volume, Length)

catch_comments<-filter(catch_comments, Species %in%c("ZZZ", "MIX", "PRI"))
#write.csv(catch_comments,"./tables/catch_comments_cont_16_03_2022.csv")# to be checked by Becca

#Merge with the WL.params

d <- merge (catch, WL.params2, all.x= T)
#head(d)
#d<-d %>% filter(year==2020 & QUARTER==4)#filter recent data
#convert length into 1cm interval
d$Length <- round (d$Length, 0)

#convert length to weight
d$Wt.At.Len_kg <- with(d ,((Length)^FACTOR.B)*FACTOR.A/1000)
d$Wt.At.Len_kg[d$Species=="SKG" & d$Length == c("31")& d$haul_id=="a91945"]<-((31)^3.286)*0.002/1000


#number of hauls sampled
d <- d%>% group_by (tripid) %>% mutate (., No.hauls.sampled = n_distinct(haul_id))# number of hauls sampled per trip

## check if more than one observer filled in data for the same haul? is this data duplicated? 
#x<-d %>% group_by(tripid,haul_id) %>% 
# mutate(.,no.observers=n_distinct(created_by))#seems some haul,trip combis were entered by two observers... I guess this is not a duplication of data but just an update

#calculate the haul RF.
# For some species in some hauls, there is no weight or length information, only number. Therefore is not possible to use those records. These records will be ignored.

#create a column with weight for all components. 
#d$haul_weight <- d$Wt.At.Len_kg
#d$haul_weight <- ifelse (is.na (d$haul_weight), d$weight, d$haul_weight )

#vessel_catch<-merge(catch,Vessel,all.x=TRUE)
#vessel_d<-merge(d,Vessel, all.x=TRUE)
#write.csv(catch,"vessel_catch_MD_20_05_21.csv")
write.csv(d,"vessel_cont_MD_16_03_22.csv")


# Calculate the RF for haul, for each species and catch component

##CAUTION!!!
#SKA and ANF might be grouped
# MLA = litter

#first focus on the retained:
#################################

# RF subsampling for each species, retained MIX = BAIT mostly
#if there is only a retained general: RF = 1 
# error: if there is only a retained sample: RF=1? there should always be a retained general but sometimes this category is missing

#RF=! 1, retained could be subsampled, could be some couldnt be measured...
#if there is a retained general and a retained sample of a species RF= based on number general/ number sample 
#In theory RF could also be based on weight or baskets in general and sample but this is not the case with the current dataset and is not taken into account in the code yet

d$gen_samp<-"Catch"
d$gen_samp [d$fate %in% c ("discardedGeneral", "retainedGeneral")] <- "General"
d$gen_samp [d$fate %in% c ("retainedSample", "discardedSample")] <- "Sample"

#head(d)

#select relevant columns
RF_haul<-select(d,tripid, vessel_name, haul_id, end_date,end_time, Species, species_comment, tags, state, catch_comp, fate, gen_samp, numberfish, weight, volume, Length, Wt.At.Len_kg)

retained<-filter(RF_haul, catch_comp=="Retained")

#prep for later conversion of weights
unique(retained$tags)
retained$Tag_RF<-0
retained$Tag_RF<-ifelse(retained$tags=="WINGS",2.09, retained$Tag_RF)
retained$Tag_RF<-ifelse(retained$tags=="U: WINGS",2.09, retained$Tag_RF)
retained$Tag_RF<-ifelse(retained$tags=="F: WINGS",2.09, retained$Tag_RF)
retained$Tag_RF<-ifelse(retained$tags=="TAILS",3,retained$Tag_RF)
retained$Tag_RF<-ifelse(retained$tags=="BAIT",1,retained$Tag_RF)
retained$Tag_RF<-ifelse(retained$tags=="U: BAIT",1,retained$Tag_RF)
#Added in according to database update (02/03/2022)
retained$Tag_RF[retained$state==10]<-3#TAILS ANF
retained$Tag_RF[retained$state==9]<-2.09#WINGS
retained$Tag_RF[retained$state==14]<-1#SHELL
retained$Tag_RF[retained$state==0]<-1#Whole heads on gutted
retained$Tag_RF[retained$Species %in% c("ZZZ", "ESB", "GUX")]<-1#other species that don't need raising? cehalopods, ESB, GUX, shellfish

unique(retained$Species) 
unique(retained$Tag_RF) 

library(data.table)

# summed over lengths
retained2<-dcast(setDT(retained), tripid+ vessel_name+ haul_id+ end_date+end_time+ Species+ catch_comp+Tag_RF~ retained$gen_samp, value.var=c('numberfish', 'weight', 'volume','Wt.At.Len_kg' ), fun.aggregate = sum, na.rm = TRUE)

# correct the total number in sample (=amount in basket + amount measured) -the number of measured fish
No_measured<- retained %>% 
  filter(!is.na(Length)) %>%
  group_by(vessel_name,tripid,haul_id,Species) %>% 
  summarize (.,No_measured_sample=sum (numberfish, na.rm=TRUE)) 

retained3<-merge(retained2, No_measured, by=c("vessel_name","tripid","haul_id","Species"), all.x= T)
retained3$numberfish_Sample_measured<-retained3$numberfish_Sample#rename column numberfish_Sample number fish sample
retained3$No_measured_sample[is.na(retained3$No_measured_sample)] <- 0
retained3$numberfish_Sample<-ifelse(retained3$numberfish_Sample_measured==retained3$No_measured_sample, retained3$numberfish_Sample_measured, retained3$numberfish_Sample_measured-retained3$No_measured_sample)

retained2<-retained3

#when number of fish general =0  and there is a number in the sample: raise sample nr with volume general/volume sample
retained2$numberfish_General <- ifelse (retained2$numberfish_General==0 & retained2$volume_Sample>0,  retained2$numberfish_Sample * (retained2$volume_General/retained2$volume_Sample), retained2$numberfish_General)
# when still number of fish general =0  and there is a number in the sample: assume RF=1 and number general = number sample
retained2$numberfish_General <- ifelse (retained2$numberfish_General==0,  retained2$numberfish_Sample, retained2$numberfish_General)


# compare number of fish general with number of fish sampled to check for not being able to measure or subsampled retained fish
#retained2$RF<-ifelse(retained2$numberfish_Sample>0,retained2$numberfish_General/retained2$numberfish_Sample, 1)
retained2$RF1<-ifelse(retained2$No_measured_sample>0,retained2$numberfish_Sample/retained2$No_measured_sample,0)
retained2$RF1<-ifelse(retained2$No_measured_sample>0,retained2$numberfish_Sample/retained2$No_measured_sample,1)
retained2$RF2<-ifelse(retained2$numberfish_Sample>0,retained2$numberfish_General/retained2$numberfish_Sample, 0)
retained2$RF2<-ifelse(retained2$numberfish_Sample>0,retained2$numberfish_General/retained2$numberfish_Sample, 1)

retained2$RF<-retained2$RF1*retained2$RF2
#check for negative raising factors:
length(which(retained2$RF<1))
length(which(retained2$RF1<1))
length(which(retained2$RF2<1))

#correct rasing factor <1 for 3 hauls (sum general + sampled)

##CORRECTIONS YEAR 2020-2021
retained2$numberfish_General[retained2$Species=="MON" & retained2$haul_id == c("a103047")]<-17
retained2$numberfish_General[retained2$Species=="MON" & retained2$haul_id == c("a103057")]<-29
retained2$numberfish_General[retained2$Species=="MON" & retained2$haul_id == c("a103025")]<-23

##CORRECTIONS selectivity work

#retained2$numberfish_General[retained2$Species=="HAD" & retained2$haul_id == c("a148703")]<-8
#retained2$No_measured_sample[retained2$Species=="HAD" & retained2$haul_id == c("a148703")]<-3
retained2$numberfish_General[retained2$Species=="LEM" & retained2$haul_id == c("a158112")]<-202
retained2$volume_General[retained2$Species=="LEM" & retained2$haul_id == c("a158112")]<-2
retained2$numberfish_Sample[retained2$Species=="LEM" & retained2$haul_id == c("a158112")]<-103
retained2$volume_Sample[retained2$Species=="LEM" & retained2$haul_id == c("a158112")]<-1

#check again:

retained2$RF1<-ifelse(retained2$No_measured_sample>0,retained2$numberfish_Sample/retained2$No_measured_sample,0)
retained2$RF1<-ifelse(retained2$No_measured_sample>0,retained2$numberfish_Sample/retained2$No_measured_sample,1)
retained2$RF2<-ifelse(retained2$numberfish_Sample>0,retained2$numberfish_General/retained2$numberfish_Sample, 0)
retained2$RF2<-ifelse(retained2$numberfish_Sample>0,retained2$numberfish_General/retained2$numberfish_Sample, 1)
#retained2$RF<-ifelse(retained2$numberfish_Sample>0,retained2$numberfish_General/retained2$numberfish_Sample, 1)
retained2$RF<-retained2$RF1*retained2$RF2

#solved?
length(which(retained2$RF2<1))#0=OK
length(which(retained2$RF1<1))# 5 with RF<1, NOT OK!!!!!!!!!!! measured is smaller than the sample
length(which(retained2$RF<1))#0=OK
# biggest raising factor is 31 OK


####ASSUMPTION to correct for the RF1<1, NOT CORRECT, something wrong with the number measured and the number in the sample!!!!
retained2$RF1 <- ifelse (retained2$RF1<1,  1, retained2$RF1)

# number of fish in general or sample is not always filled in while weight mostly is

# no weight is available for 4 records!!! t3809, a91939 ,ANF, 36  ; t3827, a91944,ANF, 25; t4009, a103057, COE, 5; t4027, a102960, MLA, 2
retained2$haul_weight<-0
#raise measured fish: 
retained2$haul_weight<-retained2$Wt.At.Len_kg_Sample* retained2$RF

#alternative method raising
#retained2$haul_weight[retained2$Species=="MON" & retained2$haul_id == c("a103047")]<-19.70+40.37
#retained2$haul_weight[retained2$Species=="MON" & retained2$haul_id == c("a103057")]<-26.50+36.37
#retained2$haul_weight[retained2$Species=="MON" & retained2$haul_id == c("a103025")]<-

#conversion for WINGS, TAILS, BAIT, heads on gutted
pres_factor <- read.csv("..\\MD\\presentation factor.csv", sep=",", header=T)
pres_factor<-filter(pres_factor,Presentation.State==c ("Heads on gutted"))
pres_factor <- pres_factor[, c("Species", "Raising.Factor")]#its actually a conversion factor!
retained2_pres<-merge(retained2,pres_factor, by="Species", all.x=TRUE)#rename BX11 Species to Species, and the general Species=Species2

#solve NA in number fish general
Check<-retained2_pres[is.na(retained2_pres$numberfish_General)]
Check<-retained2_pres[is.na(retained2_pres$weight_General)]
#retained2_pres$numberfish_General[is.na(retained2_pres$numberfish_General)] <- 0

#correct or RF=1 for WINGS/TAILS/BAIT/ZZZ/ESB/GUX
retained2_pres$haul_weight<-ifelse(retained2_pres$haul_weight ==0, retained2_pres$weight_General*retained2_pres$Tag_RF,retained2_pres$haul_weight)

#cprrect for heads on gutted
Check<-retained2_pres[is.na(retained2_pres$Raising.Factor)& retained2_pres$haul_weight==0]
unique(Check$Species)

#replace NA for Raising factor with 1
retained2_pres$Raising.Factor[is.na(retained2_pres$Raising.Factor)] <- 1

retained2_pres$haul_weight<-ifelse(retained2_pres$haul_weight ==0 & retained2_pres$numberfish_General==0, 
                                   retained2_pres$weight_General*retained2_pres$Raising.Factor,
                                   ifelse(retained2_pres$haul_weight==0 & retained2_pres$numberfish_General>0,
                                          retained2_pres$weight_General,retained2_pres$haul_weight))


length(which(retained2_pres$haul_weight<=0))

#retained2_pres$haul_weight[retained2_pres$haul_weight ==0 & is.na (retained2_pres$numberfish_General)]<- retained2_pres$weight_General*retained2_pres$Raising.Factor

#add time for Sarah skipper files
#check SKA: 
#app. 5 hauls with 0, solved

#write.csv(retained2_pres,"Z:\\BX011_WP8_ManRegimes\\Working_Area\\2021-22\\BX011V - Data Representativeness\\data and scripts\\MD\\retained_MD_02_03_2022.csv")

#raising of discards:
########################

discards<-filter(RF_haul, catch_comp=="Discards")#add date and time  
discards2<-dcast(setDT(discards), tripid+ vessel_name+haul_id+end_date+end_time+ Species+ catch_comp~ gen_samp, value.var=c('numberfish', 'weight', 'volume','Wt.At.Len_kg' ), fun.aggregate = sum, na.rm = TRUE)

## manual corrections 
discards2$weight_General[discards2$Species=="BEN" & discards2$haul_id== "a94183"]<-53
discards2$weight_General[discards2$Species=="GUX" & discards2$haul_id== "a94183"]<-306
discards2$weight_General[discards2$Species=="HAD" & discards2$haul_id== "a94183"]<-114
#discards2$weight_General[discards2$Species=="SKG" & discards3$haul_id== "a91945"]<-
discards2$weight_General[discards2$Species=="BEN" & discards2$haul_id== "a115136"]<-19
discards2$weight_General[discards2$Species=="BOF" & discards2$haul_id== "a115136"]<-6.9
discards2$weight_General[discards2$Species=="GUX" & discards2$haul_id== "a115136"]<-23
discards2$weight_General[discards2$Species=="HAD" & discards2$haul_id== "a115136"]<-21
#corrections 07/03/2022 
#MIX category
discards2<-discards2[ !(discards2$Species=="MIX" & discards2$haul_id=="a103011"),]
discards2$volume_Sample[discards2$Species=="MIX" & discards2$haul_id== "a91933"]<-0.3
discards2<-discards2[ !(discards2$Species=="MIX" & discards2$haul_id=="a144083"),]
discards2$weight_Sample[discards2$Species=="ZZZ" & discards2$haul_id== "a144083"]<-12
discards2$volume_General[discards2$Species=="ZZZ" & discards2$haul_id== "a144083"]<-1
discards2$volume_Sample[discards2$Species=="MIX" & discards2$haul_id== "a169184"]<-0.4
discards2$weight_Sample[discards2$Species=="MIX" & discards2$haul_id== "a169184"]<-0
discards2$volume_General[discards2$Species=="ZZZ" & discards2$haul_id== "a158114"]<-1
discards2<-discards2[ !(discards2$Species=="MIX" & discards2$haul_id=="a158114"),]

#number sample
discards2$numberfish_Sample[discards2$Species=="MAC" & discards2$haul_id== "a127392"]<-3

unique(discards2$vessel_name)

discards_cont<-filter(discards2, vessel_name%in% c("SWIFTSURE II" ,  "CRYSTAL SEA"  ,  "HARVEST REAPER"))
discards_select<-filter(discards2, vessel_name %in% c("COPIOUS","VENTURE"))

#first the continuous monitoring with a mixed basket
discards2<-discards_cont

## check length measured are correct
discards_l_cont<-filter(discards, vessel_name%in% c("SWIFTSURE II" ,  "CRYSTAL SEA"  ,  "HARVEST REAPER"))
No_measured<- discards_l_cont %>% 
  filter(!is.na(Length)) %>%
  group_by(vessel_name,tripid,haul_id, Species) %>% 
  summarize (.,No_measured_sample=sum (numberfish, na.rm=TRUE)) 

discards3<-merge(discards2, No_measured, by=c("vessel_name","tripid","haul_id","Species"), all.x= T)
discards3$numberfish_Sample_measured<-discards3$numberfish_Sample#rename column numberfish_Sample number fish sample
discards3$No_measured_sample[is.na(discards3$No_measured_sample)] <- 0
discards3$numberfish_Sample<-ifelse(discards3$numberfish_Sample_measured==discards3$No_measured_sample, discards3$numberfish_Sample_measured, discards3$numberfish_Sample_measured-discards3$No_measured_sample)

discards2<-discards3

##check the mix category thats supposed to be used for indicating the RF of subsamples
discardsRF<-filter(discards2, Species=="MIX")
# 6 cases with volume sample MIX=0, haul_id: a91933; a94130; a94108 ; a94109; a103126 = no sample MIX
#2 cases with Volume sample MIX=0 haul_id: a103011, a91933 --> ? all RF1 they are or left over species, or repeated species in the general fate category
# solved:haul_id: a103067 sample mix has no volume but a weight... should the weight be volume? 
discards_error<-discardsRF[discardsRF$volume_Sample==0]
discards_error

#write.csv(discards_error, "Z:\\BX011_WP8_ManRegimes\\Working_Area\\2021-22\\BX011V - Data Representativeness\\data and scripts\\MD\\discards_error1.csv")
discardsRF[is.na(discardsRF$volume_Sample)]
#discardsRF$volume_Sample[(discardsRF$volume_Sample==0)]<-1
discardsRF$RF_MIX<-ifelse(discardsRF$volume_Sample>0, discardsRF$volume_General/discardsRF$volume_Sample,NA)

discardsRF<-select(discardsRF,tripid, haul_id, RF_MIX)

discards3<-merge(discards2,discardsRF, by=c("tripid","haul_id"), all.x=TRUE)
#correct:MIX=zzz?
#discards3$Species[discards3$Species=="MIX" & discards3$haul_id == c("a94108","a94109","a94130")]<-"ZZZ"#MIX is actually left over species

length(unique(discards3$haul_id))#199 hauls of which 58 need raising
discards3$RF<-discards3$RF_MIX

check<-discards3[discards3$Wt.At.Len_kg_Sample>0 & discards3$RF_MIX >1]#1PTR, in or out of the MIX basket?

#correct for COD with a MIX category present-> assume RF=1 or RF = number General/Number Sample
discards3$numberfish_Sample<-ifelse(discards3$Species=="COD" & discards3$numberfish_Sample==0,1,discards3$numberfish_Sample)
discards3$RF<-ifelse(discards3$Species=="COD" & discards3$RF_MIX >1, (discards3$numberfish_General/discards3$numberfish_Sample), discards3$RF) 

#TO DO: Check BEN category

#correct for species that Have no Mix but are clearly subsampled based on a general and a sample number
check<-discards3[is.na(discards3$RF)]
discards3$RF<-ifelse(is.na(discards3$RF_MIX) & discards3$numberfish_General>0 & discards3$numberfish_Sample>0, discards3$numberfish_General/discards3$numberfish_Sample, discards3$RF)
check<-discards3[is.na(discards3$RF)]
#24 extra RF2250-2226

discards3$RF<-ifelse(is.na(discards3$RF) & discards3$numberfish_Sample>0 & discards3$numberfish_General==0, discards3$RF==1, discards3$RF )
check<-discards3[is.na(discards3$RF)]

discards3$RF[is.na(discards3$RF)]<-1

discards3$haul_weight<-0
discards3$Wt.At.Len_kg_Sample[is.na(discards3$Wt.At.Len_kg_Sample)]<-0
discards3$haul_weight<-discards3$Wt.At.Len_kg_Sample* discards3$RF
discards3$haul_weight<-ifelse(discards3$haul_weight==0, discards3$weight_Sample*discards3$RF,discards3$haul_weight)
discards3$haul_weight<-ifelse(discards3$haul_weight==0, discards3$weight_General,discards3$haul_weight)
length(discards3$haul_weight[is.na(discards3$haul_weight)])

#remove MIX category after all is raised
discards3<-discards3[discards3$Species!="MIX"]
check<-discards3[is.na(discards3$haul_weight)]
check[is.na(check$haul_weight)]
Check<-discards3[discards3$RF>1]

discards_cont<-discards3
#write.csv(discards3,"Z:\\BX011_WP8_ManRegimes\\Working_Area\\2021-22\\BX011V - Data Representativeness\\data and scripts\\MD\\discards_cont_MD_07_053_22.csv")

#secondly the selectivity data with length measurements
discards2<-discards_select
## check length measured are correct
discards_l_select<-filter(discards, vessel_name %in% c("COPIOUS" ,  "VENTURE"))
No_measured<- discards_l_select %>% 
  filter(!is.na(Length)) %>%
  group_by(vessel_name,tripid,haul_id,Species) %>% 
  summarize (.,No_measured_sample=sum (numberfish, na.rm=TRUE)) 

discards3<-merge(discards2, No_measured, by=c("vessel_name","tripid","haul_id","Species"), all.x= T)
discards3$numberfish_Sample_measured<-discards3$numberfish_Sample#rename column numberfish_Sample number fish sample
discards3$No_measured_sample[is.na(discards3$No_measured_sample)] <- 0
discards3$numberfish_Sample<-ifelse(discards3$numberfish_Sample_measured==discards3$No_measured_sample, discards3$numberfish_Sample_measured, discards3$numberfish_Sample_measured-discards3$No_measured_sample)

discards2<-discards3

discardsRF<-filter(discards2, Species=="MIX")
#correct:MIX=zzz

#correct for species that Have no Mix but are clearly sub sampled based on a general and a sample

# correct: 
discards2$numberfish_Sample[discards2$Species=="LEM" & discards2$haul_id== "a157674"]<-2
discards2$numberfish_General[discards2$Species=="LEM" & discards2$haul_id== "a157674"]<-2
discards2$numberfish_Sample[discards2$Species=="HAD" & discards2$haul_id== "a157674"]<-6
discards2$numberfish_General[discards2$Species=="HAD" & discards2$haul_id== "a157674"]<-7
discards2$numberfish_General[discards2$Species=="HAD" & discards2$haul_id== "a169123"]<-3

discards2$RF<-0
#when number of fish general =0  and there is a number in the sample: raise sample nr with volume general/volume sample
discards2$numberfish_General <- ifelse (discards2$numberfish_General==0 & discards2$volume_Sample>0,  discards2$numberfish_Sample * (discards2$volume_General/discards2$volume_Sample), discards2$numberfish_General)
# when still number of fish general =0  and there is a number in the sample: assume RF=1 and number general = number sample
discards2$numberfish_General <- ifelse (discards2$numberfish_General==0,  discards2$numberfish_Sample, discards2$numberfish_General)
# compare number of fish general with number of fish sampled to check for not being able to measure or subsampled retained fish

discards2$RF1<-ifelse(discards2$No_measured_sample>0,discards2$numberfish_Sample/discards2$No_measured_sample,0)
discards2$RF1<-ifelse(discards2$No_measured_sample>0,discards2$numberfish_Sample/discards2$No_measured_sample,1)
discards2$RF2<-ifelse(discards2$numberfish_Sample>0,discards2$numberfish_General/discards2$numberfish_Sample, 0)
discards2$RF2<-ifelse(discards2$numberfish_Sample>0,discards2$numberfish_General/discards2$numberfish_Sample, 1)


discards3<-discards2
discards3$RF<-discards3$RF1*discards3$RF2

length(which(discards3$RF2<1))#0=OK
length(which(discards3$RF1<1))
length(which(discards3$RF<1))

Check<-discards3[discards3$RF2<1]
#write.csv(Check, "Z:\\BX011_WP8_ManRegimes\\Working_Area\\2021-22\\BX011V - Data Representativeness\\data and scripts\\MD\\discards_error2.csv")

#3 extra RF
#discards3$RF<-ifelse(is.na(discards3$RF) & discards3$numberfish_General==0 & discards3$numberfish_Sample>0,discards3$RF==1, discards3$RF )
discards3$RF[is.na(discards3$RF)]<-1
discards3$haul_weight<-0
discards3$Wt.At.Len_kg_Sample[is.na(discards3$Wt.At.Len_kg_Sample)]<-0
discards3$haul_weight<-discards3$Wt.At.Len_kg_Sample* discards3$RF
discards3$haul_weight<-ifelse(discards3$haul_weight==0, discards3$weight_Sample*discards3$RF,discards3$haul_weight)
discards3$haul_weight<-ifelse(discards3$haul_weight==0, discards3$weight_General,discards3$haul_weight)
length(discards3$haul_weight[is.na(discards3$haul_weight)])

#remove MIX category after all is raised
discards3<-discards3[discards3$Species!="MIX"]
check<-discards3[is.na(discards3$haul_weight)]
check[is.na(check$haul_weight)]
Check<-discards3[discards3$RF>1]
#write.csv(discards3,"Z:\\BX011_WP8_ManRegimes\\Working_Area\\2021-22\\BX011V - Data Representativeness\\data and scripts\\MD\\discards_select_MD_07_03_2022.csv")

discards_select<-discards3
###########STOP RAISING #################

#rbind discards and retained

head(discards_select)
discards_select$Raising.Factor<-NA
discards_cont$Raising.Factor<-NA
discards_select$Tag_RF<-NA
discards_cont$Tag_RF<-NA
head(retained2_pres)
retained2_pres$RF_MIX<-NA
discards_select$RF_MIX<-NA
discards_cont$RF1<-NA
discards_cont$RF2<-NA
Catch<-rbind(discards_select,retained2_pres)
Catch_ALL<-rbind(Catch,discards_cont)
head(discards_cont)
head(Catch)
head(Catch_ALL)

# check hauls with only one species ESB or COD
Species_check<-Catch_ALL %>%
  group_by(vessel_name, haul_id)%>% 
  summarize(.,No.species = n_distinct(Species))
Species_check<-filter(Species_check, No.species<3)

haul_lst<-Species_check$haul_id
write.csv(Species_check,"./tables/single_species_haul_cont_MD_16_03_2022.csv")

#Catch File for JO & Anna
#Catch_JO<-select(Catch_ALL, tripid, vessel_name, haul_id, Species, catch_comp, haul_weight)

#Haul_JO<-select(locations3, vessel_rss, geartype, meshsize, end_date_trip, haul_id, start_date, end_date, end_time, endlocation_latitude, endlocation_longitude, end_ices_rectangle, end_ices_area, RF_trip, RF_Vessel)
#select continuous monitoring vessels
Catch_ALL<-filter(Catch_ALL, vessel_name%in% c("SWIFTSURE II" ,  "CRYSTAL SEA"  ,  "HARVEST REAPER"))
#remove hauls for which only one species is measured
Catch_ALL<-filter (Catch_ALL,!( haul_id %in% c(haul_lst)))


write.csv(Catch_ALL,"./tables/Catch_cont_MD_16_03_2022.csv")


Catch<-Catch_ALL
###Continuous monitoring





Vessel_catch<-Catch%>% 
  group_by(vessel_name)%>% summarize(.,No.hauls.sampled = n_distinct(haul_id),No.trips.sampled = n_distinct(tripid) ) 

#Vessel_catch_haul_type<-catch%>% 
# group_by(vessel_id, haul_type)%>% summarize(.,No.hauls.sampled = n_distinct(haul_id),No.trips.sampled = n_distinct(tripid) ) 

Vessel_fished_sampled<-merge(Vessel_trips [Vessel_trips$vessel_name%in%c("SWIFTSURE II" ,  "CRYSTAL SEA"  ,  "HARVEST REAPER"),],Vessel_catch, all.x=TRUE)
#Vessel_fished_sampled_haul_type<-merge(Vessel_trips_haul_type,Vessel_catch_haul_type, all.x=TRUE)


#add totals line
Vessel_fished_sampled<-Vessel_fished_sampled %>% bind_rows (summarise_all(., ~if(is.numeric(.)) sum(.) else "Total"))
#Vessel_fished_sampled_haul_type<-Vessel_fished_sampled_haul_type %>% bind_rows (summarise_all(., ~if(is.numeric(.)) sum(.) else "Total"))


write.csv(Vessel_fished_sampled, "./tables/Vessel_fished_sampled_Sarah_2_16_03_2022.csv")
#write.csv(Vessel_fished_sampled_haul_type, "Vessel_fished_sampled_haul_type_28_09_2021.csv")

# rename continuous data vessels: 
Catch$vessel_name[Catch$vessel_name=="SWIFTSURE II"]<-"A"
Catch$vessel_name[Catch$vessel_name=="CRYSTAL SEA"]<-"B"
Catch$vessel_name[Catch$vessel_name=="HARVEST REAPER"]<-"C"

##################
###Species List###
##################
names(Species)[names(Species) == 'main_species_code'] <- 'Species'
Species_lst<-merge(Catch,Species, all.x=TRUE)

Species_lst<-Species_lst%>%
  group_by(Species, common_name,scientific_name)%>%
  summarize(.,Weight=sum(haul_weight, na.rm=TRUE))

#write.csv(Species_lst, "./Tables/New_Species_lst_cont_16_03_2022.csv")

#Group Species
Species_lst$Group <- Species_lst$common_name# my list
Species_lst$Group [Species_lst$Species %in% c ("WAF", "MON", "ANF")] <- "Anglerfish"
Species_lst$Group [Species_lst$Species %in% c ("SQC", "SQZ", "EDC","OCT","CTC", "CTL")] <- "Cephalopods"
Species_lst$Group [Species_lst$Species %in% c ("DOG", "DGS", "LSD", "DGN", "SDS","SMH")]<- "sharks"
Species_lst$Group [Species_lst$Species %in% c ("PTR","SDR","SKA","SKG","SKF", "UNR","THR")] <- "other ray"
Species_lst$Group [Species_lst$Species %in% c ("CBX","CRE","CRW","SCR","DPD","LBE")] <- "crustaceans"
Species_lst$Group [Species_lst$Species %in% c ("TBS","WIT","FLX","SOS","SDF","FLE")] <- "other flatfish"
Species_lst$Group [Species_lst$Species %in% c ("POL","BIB","NOP","","POD", "LIN")]<-"other gadoids"
Species_lst$Group [Species_lst$Species %in% c ("HOM","MAC")]<-"MAC"
Species_lst$Group [Species_lst$Species %in% c ("MIX", "ZZZ","PIR","BEN","PRI")] <- "MIX"
Species_lst$Group [Species_lst$Species %in% c ("MLA", "MLB")] <- "Litter"
unique(Species_lst$Group)
unique(Species_lst$Species)

Species_lst$Group2 <- Species_lst$common_name # list annual report 1
Species_lst$Group2 [Species_lst$Species %in% c ("WAF", "MON", "ANF")] <- "Anglerfish"
Species_lst$Group2 [Species_lst$Species %in% c ("COD")] <- "Cod"
Species_lst$Group2 [Species_lst$Species %in% c ("BLR")] <- "Blonde ray"
Species_lst$Group2 [Species_lst$Species %in% c ("DCO")] <- "Common Dolphin"
Species_lst$Group2 [Species_lst$Species %in% c ("ESB")] <- "Seabass"
Species_lst$Group2 [Species_lst$Species %in% c ("CUR")] <- "Cuckoo Ray"
Species_lst$Group2 [Species_lst$Species %in% c ("BEN")] <- "Epibenthic Mix"
Species_lst$Group2 [Species_lst$Species %in% c ("GUX", "TUB")] <- "Gurnards"
Species_lst$Group2 [Species_lst$Species %in% c ("HAD")] <- "Haddock"
Species_lst$Group2 [Species_lst$Species %in% c ("LEM")] <- "Lemmon Sole"
Species_lst$Group2 [Species_lst$Species %in% c ("SOL")] <- "Dover Sole"
Species_lst$Group2 [Species_lst$Species %in% c ("PLE")] <- "Plaice"
Species_lst$Group2 [Species_lst$Species %in% c ("WHG")] <- "Whiting"
Species_lst$Group2 [Species_lst$Species %in% c ("CTC", "CTL")] <- "Cuttlefish"# Cephalopods
Species_lst$Group2 [Species_lst$Species %in% c ("LSD")]<-"Lesser Spotted Dogfish"
Species_lst$Group2 [Species_lst$Species %in% c ("DOG", "DGS", "DGN", "SDS","SMH", "SHK", "GAG")]<- "other sharks"#SHK, GAG addad 2022
Species_lst$Group2 [Species_lst$Species %in% c ("PTR","SDR","SKA","SKG","SKF", "UNR","THR", "ECR")] <- "other skates and rays"#ECR 2022
Species_lst$Group2 [Species_lst$Species %in% c ("CBX","CRE","CRW","SCR","DPD","LBE", "SCE", "SQC", "SQZ", "EDC","OCT", "CPZ", "CRS")] <- "other shellfish"#CPZ added 2022
Species_lst$Group2 [Species_lst$Species %in% c ("TBS","WIT","FLX","SOS","SDF","FLE", "DAB", "TUR", "BLL", "MEG")] <- "other flatfish"
Species_lst$Group2 [Species_lst$Species %in% c ("POL","BIB","NOP","POD", "LIN", "HKE")]<-"other gadoids"#OK
#Species_lst$Group2 [Species_lst$Species %in% c ("MIX", "ZZZ","PRI", "BEN")] <- "Mixed species & benthos"
Species_lst$Group2 [Species_lst$Species %in% c ("MLA", "MLB", "MLC")] <- "Litter"
Species_lst$Group2 [Species_lst$Species %in% c ("JOD")] <- "John Dory"
Species_lst$Group2 [Species_lst$Species %in% c ("BOF", "BKS", "CDT", "MUR","HOM","MAC", "HER", "COE", "PRI", "ZZZ", "ARG", "GSE", "MUX", "PLA", "PIL", "SBR", "TBR")] <- "other finfish"# ARG, GSE, MUX, "PLA", "PIL", "SBR", "TBR"
unique(Species_lst$Group2)
#write.csv(Species_lst, "Species_lst.csv")

Species_lst$Group3 <- Species_lst$common_name
Species_lst$Group3 [Species_lst$Species %in% c ("WAF", "MON", "ANF")] <- "Anglerfish"
Species_lst$Group3 [Species_lst$Species %in% c ("HAD")] <- "Haddock"
Species_lst$Group3 [Species_lst$Species %in% c ("COD")] <- "Cod"
Species_lst$Group3 [Species_lst$Species %in% c ("WHG")] <- "Whiting"
Species_lst$Group3 [Species_lst$Species %in% c ("POL","BIB","NOP","","POD", "LIN", "HKE")]<-"other gadoids"
Species_lst$Group3 [Species_lst$Species %in% c ("DCO")] <- "Common Dolphin"
Species_lst$Group3 [Species_lst$Species %in% c ("GUX", "TUB")] <- "Gurnards"
Species_lst$Group3 [Species_lst$Species %in% c ("LEM")] <- "Lemmon Sole"
Species_lst$Group3 [Species_lst$Species %in% c ("SOL")] <- "Dover Sole"
Species_lst$Group3 [Species_lst$Species %in% c ("MEG")] <- "Megrim"
Species_lst$Group3 [Species_lst$Species %in% c ("TBS","WIT","FLX","SOS","SDF","FLE", "DAB", "TUR", "BLL", "PLE")] <- "other flatfish"
Species_lst$Group3 [Species_lst$Species %in% c ("SQC", "SQZ", "EDC","OCT","CTC", "CTL")] <- "Cephalopods"
Species_lst$Group3 [Species_lst$Species %in% c ("LSD")]<- "sharks dogfish"
Species_lst$Group3 [Species_lst$Species %in% c ("DOG", "DGS", "DGN", "SDS","SMH")]<- "sharks other"
Species_lst$Group3 [Species_lst$Species %in% c ("PTR","SDR","SKA","SKG","SKF", "UNR","THR", "BLR", "CUR")] <- "skates and rays"
Species_lst$Group3 [Species_lst$Species %in% c ("CBX","CRE","CRW","SCR","DPD","LBE")] <- "crustaceans"
Species_lst$Group3 [Species_lst$Species %in% c ("HOM","MAC")]<-"(horse)Mackerel"
Species_lst$Group3 [Species_lst$Species %in% c ("BEN")] <- "Epibenthic Mix"
#Species_lst$Group3 [Species_lst$Species %in% c ("MIX", "ZZZ","PIR","PRI")] <- "MIX"
Species_lst$Group3 [Species_lst$Species %in% c ("BOF", "BKS", "CDT", "MUR","HER", "ESB", "JOD", "COE", "PRI", "ZZZ", "SBR", "SCE", "MIX")] <- "other"
Species_lst$Group3 [Species_lst$Species %in% c ("MLA", "MLB", "MLC")] <- "Litter"

#to merge with observer data: code from Ana
Species_lst$spp.group <- Species_lst$Species
Species_lst$spp.group [Species_lst$Species %in% c ("SYR", "SKT", "LNS", "SAR", "SHR", "CUR", "BLR", "THR", "PTR", "SDR", "UNR", "WSK", "ACS", "RDS", "ECR", "SGR", "SKA", "SKG", "SKT", "SGR")] <- "SKA" # skates and rays
Species_lst$spp.group [Species_lst$Species %in% c ("BSE", "BSS", "ESB")] <- "BSS" # seabass
Species_lst$spp.group[Species_lst$Species %in% c ("MON", "WAF", "ANK")] <- "ANF" # anglerfish
Species_lst$spp.group[Species_lst$Species %in% c ("GOB", "GPA")] <- "GOB" # gobies
Species_lst$spp.group[Species_lst$Species %in% c ("SEE", "CTC", "CTL", "EJE", "IAR", "SPY")] <- "CTL" # cuttlefish
Species_lst$spp.group[Species_lst$Species %in% c ("BKS", "SBC", "SBG", "SBR")] <- "FBM" # seabreams
Species_lst$spp.group[Species_lst$Species %in% c ("BOF")] <- "BOC" # boarfish
Species_lst$spp.group[Species_lst$Species %in% c ("HOM")] <- "JAX" # horse mackerel
Species_lst$spp.group[Species_lst$Species %in% c ("GUR", "GUL", "GUX", "GUG", "GUS", "GUM", "GUN", "TUB")] <- "GUX" # gurnards
Species_lst$spp.group[Species_lst$Species %in% c ("SQC", "NSQ", "LLV", "SQZ", "SQE", "SQR", "SQF", "OMZ", "TDQ", "OME")] <- "SQC"   ## lolicon
Species_lst$spp.group[Species_lst$Species %in% c ("MEG", "LBI")] <- "LEZ" #megrims
Species_lst$spp.group[Species_lst$Species %in% c ("MUR", "MBB")] <- "MUR" # mullets
Species_lst$spp.group[Species_lst$Species %in% c ("MTG", "MUL")] <- "MUL" # mullets
Species_lst$spp.group[Species_lst$Species %in% c ("LSD")] <- "SYC" # lesser spotted dogfish
Species_lst$spp.group[Species_lst$Species %in% c ("SLO", "CRW")] <- "CRW" # Crawfish

write.csv(Species_lst, "./tables/New_Species_lst.csv")
unique(Species_lst$spp.group)

Species_lst2 <- Species_lst$Species [order (Species_lst$Weight, decreasing = TRUE)]
#Species_lst2<-Species_lst2[1:15]

##############
####Results###
##############

#Correct odd hauls with only cod catch or ESB catch
Catch<- filter(Catch, !(haul_id %in% c("a94196", "a94199","a94202", "a94201")))%>% droplevels()
Catch<- filter(Catch, !(Species %in% c("MLA", "MLB", "MLC")))%>% droplevels()
Catch$haul_weight[is.na(Catch$haul_weight)]<-0

Catch_tbl<-dcast(setDT(Catch), tripid+ vessel_name+ haul_id+ end_date+end_time+ Species ~ catch_comp, value.var=c('haul_weight'), fun.aggregate = sum)
Catch_tbl$Catch<-Catch_tbl$Discards+Catch_tbl$Retained

Catch_tbl2<-Catch_tbl%>%
  group_by(vessel_name)%>%
  summarize(., Discards = sum (Discards, na.rm=TRUE), Retained=sum(Retained, na.rm=TRUE), Catch=sum(Catch, na.rm=TRUE),No.hauls=n_distinct(haul_id))%>%
  mutate(DR=Discards/Retained, Avg_Discards= Discards/No.hauls, Avg_Retained=Retained/No.hauls,Avg_Catch=Catch/No.hauls)

Catch_tbl3<-Catch_tbl%>%
  group_by(vessel_name, haul_id)%>%
  summarize(., Discards = sum (Discards, na.rm=TRUE), Retained=sum(Retained, na.rm=TRUE), Catch=sum(Catch, na.rm=TRUE))%>%
  mutate(DR=Discards/Retained, na.rm=TRUE)

Catch_tbl4<-Catch_tbl3%>%
  group_by(vessel_name)%>%
  summarize(., TDiscards = sum (Discards, na.rm=TRUE), TRetained=sum(Retained, na.rm=TRUE), TCatch=sum(Catch, na.rm=TRUE), Av_DR=mean(DR, na.rm=TRUE),SD_DR=sd(DR, na.rm=TRUE), Av_Discards=mean(Discards, na.rm=TRUE), SD_Discards=sd(Discards, na.rm=TRUE), Av_Retained=mean(Retained, na.rm=TRUE), SD_Retained=sd(Retained, na.rm=TRUE), AV_Catch=mean(Catch, na.rm=TRUE), SD_Catch=sd(Catch, na.rm=TRUE), No.hauls=n_distinct(haul_id))%>%
  mutate(TDR=TDiscards/TRetained, Avg_Discards=TDiscards/No.hauls, Avg_Retained=TRetained/No.hauls,Avg_Catch=TCatch/No.hauls)

Catch_tbl3<-Catch_tbl%>%
  group_by(vessel_name, haul_id)%>%
  summarize(., Discards = sum (Discards, na.rm=TRUE), Retained=sum(Retained, na.rm=TRUE), Catch=sum(Catch, na.rm=TRUE))%>%
  mutate(DR=Discards/Catch, na.rm=TRUE)

Catch_tbl4<-Catch_tbl3%>%
  group_by(vessel_name)%>%
  summarize(., TDiscards = sum (Discards, na.rm=TRUE), TRetained=sum(Retained, na.rm=TRUE), TCatch=sum(Catch, na.rm=TRUE), Av_DR=mean(DR, na.rm=TRUE),SD_DR=sd(DR, na.rm=TRUE), Av_Discards=mean(Discards, na.rm=TRUE), SD_Discards=sd(Discards, na.rm=TRUE), Av_Retained=mean(Retained, na.rm=TRUE), SD_Retained=sd(Retained, na.rm=TRUE), AV_Catch=mean(Catch, na.rm=TRUE), SD_Catch=sd(Catch, na.rm=TRUE), No.hauls=n_distinct(haul_id))%>%
  mutate(TDR=TDiscards/TCatch, Avg_Discards=TDiscards/No.hauls, Avg_Retained=TRetained/No.hauls,Avg_Catch=TCatch/No.hauls)


write.csv(Catch_tbl4, "./tables/Catch_DR_tCatch_17_03_2022.csv")


ggplot(Catch_tbl3,aes(x=vessel_name,y=Catch, fill=vessel_name))+
  geom_boxplot(outlier.shape = NA)+
  geom_jitter()+
  stat_summary(fun.y="mean", shape=4)+
  labs(x = " ",y="Catch (kg)")+
  theme (axis.title.x=element_blank(), legend.position = "none")
ggsave("./plots/Box_Catch.jpg")


ggplot(Catch_tbl3,aes(x=vessel_name,y=Discards, fill=vessel_name))+
  geom_boxplot(outlier.shape = NA)+
  geom_jitter()+
  stat_summary(fun.y="mean", shape=4)+
  labs(x = " ",y="Discards (kg)")+
  theme (axis.title.x=element_blank(), legend.position = "none")
ggsave("./plots/Box_Discards.jpg")

ggplot(Catch_tbl3,aes(x=vessel_name,y=Retained, fill=vessel_name))+
  geom_boxplot(outlier.shape = NA)+
  geom_jitter()+
  stat_summary(fun.y="mean", shape=4)+
  labs(x = " ",y="Retained (kg)")+
  theme (axis.title.x=element_blank(), legend.position = "none")
ggsave("./plots/Box_Retained.jpg")

ggplot(Catch_tbl3,aes(x=vessel_name,y=DR, fill=vessel_name))+
  geom_boxplot()+
  geom_jitter()+
  stat_summary(fun.y="mean", shape=4)+
  labs(x = " ",y="Discard rate %")+
  theme (axis.title.x=element_blank(), legend.position = "none")
ggsave("./plots/Box_DR_Tcatch.jpg")

###################
###Species level###
###################

Catch_comp<-Catch%>%
  group_by(vessel_name,Species,catch_comp)%>%
  summarise(., weight=sum(haul_weight,na.rm=T))

ggplot(Catch_comp,aes(x=Species,y=weight, fill=catch_comp))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(vessel_name~. ,scales="free")
#("haul_weight_All.jpg")

ggplot(Catch_comp,aes(x=catch_comp,y=weight, fill=Species))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(.~vessel_name ,scales="free")
#ggsave("haul_weight_All.jpg")

#too many species:
# 15
#Catch_compSPP<-subset(Catch_comp, Species %in% Species_lst2 [1:20])
ggplot(data=subset(Catch_comp,Species %in% Species_lst2[1:20]),aes(x=Species,y=weight, fill=catch_comp))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(vessel_name~. ,scales="free")+
  theme (axis.text.x=element_text(size=8, angle = 90, hjust = 0,vjust=0.5))
ggsave("Species_top20_D&R.jpg")

ggplot(data=subset(Catch_comp,Species %in% Species_lst2[1:20]),aes(x=catch_comp,y=weight, fill=Species))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(.~vessel_name ,scales="free")
ggsave("Species_top20_weight.jpg")

# regrouping 
Catch_comp_group<-merge(Catch_comp,Species_lst, all.x=TRUE)
Catch_comp_group1<-Catch_comp_group%>%
  group_by(vessel_name,Group,catch_comp)%>%
  summarise(., weight=sum(weight,na.rm=T))

ggplot(Catch_comp_group1,aes(x=Group,y=weight, fill=catch_comp))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(vessel_name~. ,scales="free")+
  theme (axis.text.x=element_text(size=8, angle = 90, hjust = 0,vjust=0.5))
ggsave("Species_Group1_R&D.jpg")

ggplot(Catch_comp_group1 ,aes(x=catch_comp,y=weight, fill=Group))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(.~vessel_name ,scales="free")
ggsave("Species_Group1_Weight.jpg")

Catch_comp_group2<-Catch_comp_group%>%
  group_by(vessel_name)%>%
  mutate(TCatch=sum(weight,na.rm=TRUE))%>%
  group_by(vessel_name,Group2,catch_comp,TCatch)%>%
  summarise(., weight=sum(weight,na.rm=T))%>%
  mutate(perc=(weight/TCatch)*100)

#Catch_comp_group2$Group2<-factor(Catch_comp_group2$Group2, levels=c("Haddock","Gurnards","other skates and rays","Anglerfish","sharks", "Whiting", "other finfish","other flatfish", "Cuttlefish", "Lemmon Sole", "other gadoids", "other shellfish", "Epibenthic Mix", "Cuckoo Ray"))
ggplot(Catch_comp_group2,aes(x=Group2,y=weight, fill=catch_comp))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(vessel_name~. ,scales="free")+
  labs(x = " ",y="Weight (kg)")+
  guides (fill=guide_legend(title=NULL))+
  theme (axis.text.x=element_text(size=8, angle = 90, hjust = 0,vjust=0.5), axis.title.x=element_blank())
ggsave("Species_Group2_R&D.jpg")


ggplot(Catch_comp_group2 ,aes(x=catch_comp,y=weight, fill=Group2))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(.~vessel_name ,scales="free")+
  labs(x = " ",y="Weight (kg)")+
  guides (fill=guide_legend(title=NULL))+
  theme (axis.title.x=element_blank())+
  scale_fill_manual(values=c('#e6194b', '#3cb44b', '#ffe119', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe','#4363d8', '#008080', '#808080','#e6beff', '#9a6324', '#fffac8', '#800000', '#ffd8b1','#aaffc3', '#808000', '#ffffff', '#000000', '#000075'))
ggsave("Species_Group2_Weight_17_03_2022.jpg")

ggplot(Catch_comp_group2 ,aes(x=catch_comp,y=perc, fill=Group2))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(.~vessel_name ,scales="free")+
  labs(x = " ",y="% Total Catch")+
  guides (fill=guide_legend(title=NULL))+
  theme (axis.title.x=element_blank())+
  scale_fill_manual(values=c('#e6194b', '#3cb44b', '#ffe119', '#f58231', '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe','#4363d8', '#008080', '#808080','#e6beff', '#9a6324', '#fffac8', '#800000', '#ffd8b1','#aaffc3', '#808000', '#ffffff', '#000000', '#000075'))
ggsave("Species_Group2_perc_17_03_2022.jpg")
#standardize between vessels for number of hauls

Catch_comp_group3<-Catch_comp_group2%>%
  group_by(vessel_name)%>%
  mutate(TCatch=sum(weight,na.rm=TRUE))%>%
  group_by(vessel_name,Group2, TCatch)%>%
  summarise(., weight=sum(weight,na.rm=T))%>%
  mutate(perc=(weight/TCatch)*100)
Catch_comp_group3<-dcast(setDT(Catch_comp_group3), Group2~ vessel_name, value.var=c('weight', 'perc'), fun.aggregate = sum)

write.csv(Catch_comp_group3, "key_Species_vessel_3.csv")

Catch_comp<-Catch%>% 
  group_by(vessel_name)%>%
  mutate(No.hauls=n_distinct(haul_id), TCatch=sum(haul_weight,na.rm=TRUE))%>%
  group_by(vessel_name,Species,catch_comp, No.hauls, TCatch)%>%
  summarise(., weight=sum(haul_weight,na.rm=T))%>%
  mutate(weight/No.hauls, perc=(weight/TCatch)*100)
Catch_comp_group<-merge(Catch_comp,Species_lst, all.x=TRUE)

Catch_comp_group_D<-dcast(setDT(Catch_comp_group), Species+common_name+scientific_name+ Group2+catch_comp~ vessel_name, value.var=c('weight','weight/No.hauls', 'perc'), fun.aggregate = sum)

write.csv(Catch_comp, "Species_vessel.csv")
write.csv(Catch_comp_group_D, "Species_vessel_D.csv")

###############################
#relative catch composition### 
##############################  

RF<-RF_haul%>%
  group_by (tripid, haul_id, Species, catch_comp, fate, gen_samp)%>%
  summarise (., n = sum (numberfish, na.rm = T), sampled_weight = sum (Wt.At.Len_kg, na.rm = T))

head(d)
RF_haul <- d %>%
  group_by (haul_id, Species, catch_comp)%>%
  summarise (., total_weight = sum (haul_weight, na.rm = T), sampled_weight = sum (Wt.At.Len_kg, na.rm = T)) %>% mutate (., RF_haul_try = total_weight / sampled_weight)



RF_haul_fate <- d %>%
  group_by (haul_id, Species, catch_comp, fate)%>%
  summarise (., total_weight = sum (haul_weight, na.rm = T), sampled_weight = sum (Wt.At.Len_kg, na.rm = T)) %>% mutate (., RF_haul = total_weight / sampled_weight)

library(ggplot2)
ggplot(RF_haul,aes(x=Species,y=total_weight,fill=catch_comp))+
  geom_bar(stat="identity", colour="black")
ggsave("haul_weight2.jpg")

ggplot(RF_haul,aes(x=catch_comp,y=total_weight,fill=Species))+
  geom_bar(stat="identity", colour="black") #+
#facet_grid(.~fate, scales="free", space="free")+
#theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1), strip.text.x = element_text(angle=90))
#ggsave("5Ylandings.regio.jpg")
ggsave("comp_weight2.jpg")

ggplot(RF_haul_fate,aes(x=fate,y=total_weight,fill=Species))+
  geom_bar(stat="identity", colour="black") #+
#facet_grid(.~fate, scales="free", space="free")+
#theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1), strip.text.x = element_text(angle=90))
#ggsave("5Ylandings.regio.jpg")
ggsave("fate_weight2.jpg")

#RF_haul_MD <- d %>%
# group_by (haul_id, Species, catch_comp)%>%
# summarise (., total_weight = sum (weight, na.rm = T), sampled_weight = sum (Wt.At.Len_kg, na.rm = T)) %>% mutate (., RF_haul = total_weight / sampled_weight)

#####################################
### 4 Lengths Measured  - not raised###
#####################################

#extract only data associated with a length
lenA <- d[!is.na(d$Length),]
length(which(!is.na(d$Length)))
Length_measured<-lenA%>% 
  group_by(vessel_name, vessel_id, Species, catch_comp)%>% summarize(.,numberfish= sum(numberfish), No.hauls.fished = n_distinct(haul_id),No.trips.fished = n_distinct(tripid) )
write.csv(Length_measured,"Length_measured_MD2_24_03_22.csv")

Length_freq<-lenA%>% 
  group_by(vessel_name, Species,catch_comp, Length)%>% 
  summarize(.,numberfish= sum(numberfish), No.hauls.fished = n_distinct(haul_id),No.trips.fished = n_distinct(tripid) )
##correct one monkfish of 370cm? 
#Length_freq$Length[Length_freq$Species=="MON" & Length_freq$Length == 370]<-37

library(ggplot2)
ggplot(Length_freq,aes(x=Length,y=numberfish, fill=catch_comp))+
  geom_bar(stat="identity")+
  facet_grid(vessel_name~Species, scales="free_y")#,space="free" 
ggsave("lenght_freq2_19_05.jpg")

#######################
###Raised COD
#######################
#retained<-filter(RF_haul, catch_comp=="Retained" & Species=="COD")
retained<-filter(RF_haul, Species=="COD")
#retained<-filter(RF_haul, Species=="MON")
#prep for later conversion of weights
retained$Tag_RF<-0

library(data.table)
retained2<-dcast(setDT(retained), vessel_name+ haul_id+ Species+ catch_comp~ retained$gen_samp, value.var=c('numberfish','Wt.At.Len_kg' ), fun.aggregate = sum)
# summed over lengths
# when number of fish general =0  and there is a number in the sample: assume RF=1 and number general = number sample
retained2$numberfish_General <- ifelse (retained2$numberfish_General==0,  retained2$numberfish_Sample, retained2$numberfish_General)
# compare number of fish general with number of fish sampled to check for not being able to measure or subsampled retained fish
retained2$RF<-ifelse(retained2$numberfish_Sample>0,retained2$numberfish_General/retained2$numberfish_Sample, 1)
#check for negative raising factors:
length(which(retained2$RF<1))
#correct rasing factor <1 for 3 hauls (sum genreal + sampled)
#retained2$numberfish_General[retained2$Species=="MON" & retained2$haul_id == c("a103047")]<-17

# biggest raising factor is 2, OK
# number of fish in general or sample is not always filled in while weight mostly is
# no weight is available for 4 records!!! t3809, a91939 ,ANF, 36  ; t3827, a91944,ANF, 25; t4009, a103057, COE, 5; t4027, a102960, MLA, 2
retained2<-select(retained2,haul_id,catch_comp,RF)
#retained2$haul_number<-0

#raise measured fish: 
retained2<-merge(retained,retained2,by=c ("haul_id", "catch_comp"), all.x=TRUE)
retained2 <- retained2[!is.na(retained2$Length),]
retained2$haul_number<-retained2$numberfish* retained2$RF
lenA<-retained2

lenA$vessel_name[lenA$vessel_name=="SWIFTSURE II"]<-"A"
lenA$vessel_name[lenA$vessel_name=="CRYSTAL SEA"]<-"B"
lenA$vessel_name[lenA$vessel_name=="HARVEST REAPER"]<-"C"

Length_freq<-lenA%>% 
  group_by(vessel_name, Species,catch_comp, Length)%>% 
  summarize(.,numberfish= sum(haul_number), No.hauls.fished = n_distinct(haul_id),No.trips.fished = n_distinct(tripid) )


library(ggplot2)
ggplot(Length_freq,aes(x=Length,y=numberfish, fill=catch_comp))+
  geom_bar(stat="identity")+
  facet_grid(vessel_name~Species, scales="free_y")+#,space="free" 
  labs(x = "Length (cm)",y="number of fish")+
  guides (fill=guide_legend(title=NULL))
ggsave("lenght_freqCOD2_19_05.jpg")

##################################################
###5. Calculate the raising factor for the trip###
##################################################
#get the total number of hauls by trip

trip <- dbGetQuery(con, "SELECT bx011.haul.tripid, bx011.haul.haul_id, bx011.haul.geartype,
                                bx011.haul.start_ices_rectangle, 
                                bx011.gear.meshsize, bx011.gear.netlength

                          FROM bx011.haul LEFT JOIN bx011.gear ON bx011.gear.haul_id = bx011.haul.haul_id")

tripx<-trip%>% filter(tripid=="t3667")
#calculate the number of hauls fished in each trip

trip <- trip %>%
  group_by(tripid) %>% mutate (., No.hauls.fished = n_distinct(haul_id))

# merge it with catch data

d1 <- merge (d, trip, all.x = T)

# calculate the raising factor for trip
d1$RF_trip <- with (d1, No.hauls.fished / No.hauls.sampled)# 3 hauls fished for the trip

### 3. Raise the length 
# get the data for which there are length data only
len <- d1[!is.na(d1$Length),]

#merge the length data with the RF_haul
len1 <- merge (len, RF_haul, all.x=T )

len1$N.at.len.raised.haul <- with (len1, numberfish * RF_haul)
len1$N.at.len.raised.trip <- with (len1, N.at.len.raised.haul  * RF_trip)

## summarise the length data by trip
len2 <- len1 %>%
  group_by (tripid, QUARTER, year, ices_area, geartype, catch_comp, Species, Sex, Length, Wt.At.Len_kg) %>%
  summarise(., N.at.len.raised.trip = sum (N.at.len.raised.trip, na.rm = T), N.at.len.measured = sum(numberfish, na.rm = T))
len2<- as.data.frame(len2)
head(len2)

ggplot(len2,aes(x=Length,y=N.at.len.raised.trip, fill=catch_comp))+
  geom_bar(stat="identity", colour="black")+
  facet_grid(Species~., scales="free", space="free")
ggsave("len_trip raised2.jpg")

###4. calculate the discards and retained for a trip
catch_trip <- d1 %>%
  group_by(tripid,Species, catch_comp, RF_trip) %>%
  summarise (., total_weight_haul = sum(haul_weight, na.rm = T)) %>% mutate (., Trip_weight = total_weight_haul * RF_trip)

catch_trip <- as.data.frame (catch_trip)
head(catch_trip)


#########STOP #########################COPIED ABovE#############

###5.calculte the number of trips and hauls fished and sampled per vessel
trip <- dbGetQuery(con, "SELECT bx011.haul.tripid, bx011.haul.haul_id, bx011.haul.geartype,
                                bx011.haul.start_ices_rectangle, 
                                bx011.gear.meshsize, bx011.gear.netlength

                          FROM bx011.haul LEFT JOIN bx011.gear ON bx011.gear.haul_id = bx011.haul.haul_id")



location <- dbGetQuery(con, "SELECT bx011.trip.tripid, bx011.trip.vessel_id, bx011.haul.haul_id,
                                bx011.haul.startlocation_latitude, bx011.haul.startlocation_longitude
                                

                          FROM bx011.trip LEFT JOIN bx011.haul ON bx011.trip.tripid = bx011.haul.tripid")

Vessel <- dbGetQuery(con, "SELECT bx011.vessel.vessel_name, bx011.trip.tripid,bx011.trip.start_date, bx011.trip.vessel_id
                                

                          FROM bx011.trip LEFT JOIN bx011.vessel ON bx011.trip.vessel_id = bx011.vessel.vessel_id")
Vessel <- dbGetQuery(con, "SELECT bx011.vessel.vessel_name, bx011.trip.tripid,bx011.trip.start_date, bx011.trip.vessel_id
                                

                          FROM bx011.vessel LEFT JOIN bx011.trip ON bx011.vessel.vessel_id = bx011.trip.vessel_id")

TRIP<-merge(trip, Vessel, all.x = TRUE)
#TRIP<-TRIP%>% filter(start_date>= "2020-10-01")

#TRIP<-merge(TRIP,location, all.y=TRUE)#some hauls not? 

Vessel_trips<-TRIP %>%
  group_by(vessel_name, vessel_id)%>% summarize(.,No.hauls.fished = n_distinct(haul_id),No.trips.fished = n_distinct(tripid) ) # do we need the name of the vessels or rather id?

CATCH<-merge(catch,Vessel,all.x=TRUE)

#CATCH<-CATCH%>% filter(start_date>= "2020-10-01")#All before october 2020 is dummy data

Vessel_catch<-CATCH%>% 
  group_by(vessel_name, vessel_id)%>% summarize(.,No.hauls.sampled = n_distinct(haul_id),No.trips.sampled = n_distinct(tripid) ) 

Vessel_fished_sampled<-merge(Vessel_trips,Vessel_catch, all.x=TRUE)

#write.csv(Vessel_fished_sampled, "Vessel_fished_sampled_17_05_2021.csv")
#1 trip and 1haul sampled? 
# add hauls rejected? 

## length measured fish


d <- merge (CATCH, WL.params2, all.x= T)
#head(d)
#d<-d %>% filter(year==2020 & QUARTER==4)#filter recent data
#convert length into 1cm interval
d$Length <- round (d$Length, 0)

#convert length to weight
d$Wt.At.Len_kg <- with(d ,((Length)^FACTOR.B)*FACTOR.A/1000)

#number of hauls sampled
d <- d%>% group_by (tripid) %>% mutate (., No.hauls.sampled = n_distinct(haul_id))

#calculate the haul RF.
# For some species in some hauls, there is no weight or length information, only number. Therefore is not possible to do use those records. These records will be ignored.

#create a column with weight for all components. 
d$haul_weight <- d$Wt.At.Len_kg
d$haul_weight <- ifelse (is.na (d$haul_weight), d$weight, d$haul_weight )

#write.csv(catch,"catch_MD.csv")
#write.csv(d,"d_vessel_MD.csv")
# Calculate the RF for haul, for each species and catch component
RF_haul <- d %>%
  group_by (haul_id, Species, catch_comp)%>%
  summarise (., total_weight = sum (haul_weight, na.rm = T), sampled_weight = sum (Wt.At.Len_kg, na.rm = T)) %>% mutate (., RF_haul = total_weight / sampled_weight)

RF_haul_fate <- d %>%
  group_by (haul_id, Species, catch_comp, fate)%>%
  summarise (., total_weight = sum (haul_weight, na.rm = T), sampled_weight = sum (Wt.At.Len_kg, na.rm = T)) %>% mutate (., RF_haul = total_weight / sampled_weight)




