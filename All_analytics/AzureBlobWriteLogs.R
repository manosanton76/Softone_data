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

# List files
list_blobs(cont)

# Upload files
upload_blob(cont, "forecasting_all.csv")

# if you want only the filenames
list_blobs(cont, info="name")

# download file
download_blob(cont, "template.csv", overwrite=TRUE)


# Download bulk files -----------------------------------------------------------
library(tidyverse)
library(tictoc)

# Get the filenames
tt <- list_blobs(cont, info="name")

tic()
# download files
tt %>%
  map(~ download_blob(cont, .x, overwrite=TRUE))
toc()


tic()
# download files
tt %>%
  map(~ try(multidownload_blob(cont, .x, overwrite=FALSE,
                           max_concurrent_transfers = 10)))
toc()


tt %>%
  as_data_frame() %>%
  rename(new = value) %>%
  arrange(desc(new)) %>%
  separate(new, c("a", "b"), sep = '\\.' ) %>%
  group_by(b) %>%
  count()


# Delete bulk Files ------------------------------------------------------------

# Create a temporary token to delete blobs
cont <- blob_container(
  "https://softonebilogs.blob.core.windows.net/softonebilogs",
  sas="sp=racwdli&st=2022-04-20T09:36:26Z&se=2022-04-21T17:36:26Z&spr=https&sv=2020-08-04&sr=c&sig=EZN3ZGqQtDO%2BWLHchdD4occVuNqmXEgPcaN%2BD1ukuMQ%3D")


# Get the filenames
tt <- list_blobs(cont, info="name")

# Filter the blobs for deletion
tt1 <-
  tt %>%
  as_data_frame() %>%
  rename(new = value) %>%
  filter(str_detect(new, "txt") | str_detect(new, "log")) %>%
  pull(new)


# delete blobs
tt1 %>%
  map(~ delete_blob(cont, .x, confirm = FALSE))

