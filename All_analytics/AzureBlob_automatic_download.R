#' https://blog.revolutionanalytics.com/2018/12/azurestor.html
#'

library(AzureStor)
library(tidyverse)

# Create a storage account & blob storage --------------------------------------
#' Create and go to settings and right click on Shared access tokens and generate
#' a SAS token & use the blob token ....


# Upload & list files in blobs --------------------------------------------------

# Define the blob storage
cont <- blob_container(
  "https://softonebilogs.blob.core.windows.net/softonebilogs",
  sas="sp=racwli&st=2022-04-01T13:42:50Z&se=2022-12-31T22:42:50Z&spr=https&sv=2020-08-04&sr=c&sig=mIpQrdjrjRmaOeWxR8j24k%2BVtH1JjajPk5CtOi6s9Cs%3D")

# Get the filenames
tt <- list_blobs(cont, info="name")

setwd("/mnt/c/Users/eam/OneDrive - Softone Technologies S.A/Business_Analytics/00_Analytics_Projects/12_Production/Logs")

# download files
tt %>%
  map(~ try(multidownload_blob(cont, .x, overwrite=FALSE,
                               max_concurrent_transfers = 10)))

