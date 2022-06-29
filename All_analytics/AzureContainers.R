
library(AzureContainers)
library(AzureRMR)
library(lubridate)


# # Interactive login
# rg <- AzureRMR::get_azure_login()$
#   get_subscription("c7095c76-bf32-4f12-9e82-da7b408e7a45")$
#   get_resource_group("AnalyticsDevTest")

# App login
rg <- AzureRMR::create_azure_login(tenant = 'f99f92d3-7e2b-4820-858a-b629fad639e0',
                                   app = 'e9052de9-8d51-4d11-89ed-f0918b9ad241',
                                   password = '.c27Q~cLkQ~qIv7TZzT2gp6rx9WMhZVAo2Cci',
                                   auth_type = 'client_credentials'
                                    )$
  get_subscription("c7095c76-bf32-4f12-9e82-da7b408e7a45")$
  get_resource_group("AnalyticsDevTest")



# Azure Container registry credentials
deployreg <- aci_creds(server = "soft1analytics01.azurecr.io",
                       username = "soft1analytics01",
                       password = "4caGNUU0mCilcSfdduvL+Xl2caITb7ao")

# env_variables <- list(ajobid='900', checkid='3796164566156386153', serial='01142463802517') # Demo Installation

# env_variables <- list(ajobid='938', checkid='7439125618718869410', serial='01100253604408') # Pantopoleia Large...

env_variables <- list(ajobid='925', checkid='3713551328896049762', serial='01102467074017') # Georgakopoulos

aci_name <- paste("jobid", env_variables$ajobid, sep = "")

rg$create_aci(name = aci_name,
              image="soft1analytics01.azurecr.io/analytics:1.02",
              registry_creds=deployreg,
              cores=2,
              # memory=14,
              memory=16, # Max Ram
              os = "Linux",
              env_vars = env_variables,
              restart = "OnFailure")



aci_status <- ""

while (aci_status != "Terminated") {
  myaci <- rg$get_aci(aci_name)
  aci_status <- myaci[["properties"]][["containers"]][[1]][["properties"]][["instanceView"]][["currentState"]][["state"]]
  Sys.sleep(10)
}

Sys.sleep(5)

rg$delete_aci(aci_name, confirm=FALSE)


# myaci <- rg$get_aci(aci_name)
#
# myaci[["properties"]][["containers"]][[1]][["properties"]][["instanceView"]][["currentState"]][["state"]]
#
#

# rg$new()
# myaci <- rg$get_aci("mycontainer")
# myaci$stop()
# myaci$restart()


