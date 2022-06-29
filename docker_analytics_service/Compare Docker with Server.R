

library(tidyverse)


# DOCKER Files -----------------------------------------------------------------
path <- "C:/Users/eam/OneDrive - Softone Technologies S.A/Επιφάνεια εργασίας/Docker/"

AssociationsAllCompanies_Docker <- 
  read.csv(paste(path, "AssociationsAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

CustomerSegmentationAllCompanies_Docker <- 
  read.csv(paste(path, "CustomerSegmentationAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)

CustomerSegmentationPerCompany_Docker <- 
  read.csv(paste(path, "CustomerSegmentationPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)

ForecastingPerProductAllCompanies_Docker <- 
  read.csv(paste(path, "ForecastingPerProductAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingReceiptsAllCompanies_Docker <- 
  read.csv(paste(path, "ForecastingReceiptsAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingReceiptsPerCompany_Docker <- 
  read.csv(paste(path, "ForecastingReceiptsPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingSalesAllCompanies_Docker <- 
  read.csv(paste(path, "ForecastingSalesAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingSalesPerCompany_Docker <- 
  read.csv(paste(path, "ForecastingSalesPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ItemSegmentationAllCompanies_Docker <- 
  read.csv(paste(path, "ItemSegmentationAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)

ItemSegmentationPerCompany_Docker <- 
  read.csv(paste(path, "ItemSegmentationPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)


# SERVER Files -----------------------------------------------------------------
path <- "C:/Users/eam/OneDrive - Softone Technologies S.A/Επιφάνεια εργασίας/Server/"

AssociationsAllCompanies_Server <- 
  read.csv(paste(path, "AssociationsAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

CustomerSegmentationAllCompanies_Server <- 
  read.csv(paste(path, "CustomerSegmentationAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)

CustomerSegmentationPerCompany_Server <- 
  read.csv(paste(path, "CustomerSegmentationPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)

ForecastingPerProductAllCompanies_Server <- 
  read.csv(paste(path, "ForecastingPerProductAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingReceiptsAllCompanies_Server <- 
  read.csv(paste(path, "ForecastingReceiptsAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingReceiptsPerCompany_Server <- 
  read.csv(paste(path, "ForecastingReceiptsPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingSalesAllCompanies_Server <- 
  read.csv(paste(path, "ForecastingSalesAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ForecastingSalesPerCompany_Server <- 
  read.csv(paste(path, "ForecastingSalesPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated)

ItemSegmentationAllCompanies_Server <- 
  read.csv(paste(path, "ItemSegmentationAllCompanies.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)

ItemSegmentationPerCompany_Server <- 
  read.csv(paste(path, "ItemSegmentationPerCompany.csv", sep = ""), sep=";") %>% 
  select(-datetimecreated, -pc1, -pc2)

# Check for equality -----------------------------------------------------------
all_equal(AssociationsAllCompanies_Docker, AssociationsAllCompanies_Server) # OK
all_equal(ForecastingPerProductAllCompanies_Docker, ForecastingPerProductAllCompanies_Server) # OK
all_equal(ForecastingReceiptsAllCompanies_Docker, ForecastingReceiptsAllCompanies_Server) # OK
all_equal(ForecastingReceiptsPerCompany_Docker, ForecastingReceiptsPerCompany_Server) # OK
all_equal(ForecastingSalesAllCompanies_Docker, ForecastingSalesAllCompanies_Server) # OK
all_equal(ForecastingSalesPerCompany_Docker, ForecastingSalesPerCompany_Server) # OK


all_equal(CustomerSegmentationAllCompanies_Docker, CustomerSegmentationAllCompanies_Server)
all_equal(CustomerSegmentationPerCompany_Docker, CustomerSegmentationPerCompany_Server)
all_equal(ItemSegmentationAllCompanies_Docker, ItemSegmentationAllCompanies_Server)
all_equal(ItemSegmentationPerCompany_Docker, ItemSegmentationPerCompany_Server)




