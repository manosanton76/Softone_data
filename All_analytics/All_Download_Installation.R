

library(tidyverse)
library(lubridate)
library(foreach)
library(doParallel)
library(tictoc)
library(Soft1R)
library(forecast)
library(sweep)
library(tidyquant)
library(AzureAuth)
library(AzureStor)
library(data.table)
library(parsedate)
library(anomalize)
library(cluster)
library(recommenderlab)
library(arules)
library(arulesViz)
library(prophet)



datalake <- 0

code <- "211999020512"


  if (datalake == 0 ) {
    token_path <- "https://s1datalakeprod01.blob.core.windows.net"
    main_path <- "https://s1datalakeprod01.dfs.core.windows.net"
  } else {
    token_path <- "https://s1azdatalake01.blob.core.windows.net"
    main_path <- "https://s1azdatalake01.dfs.core.windows.net"
  }


  # 1. Get Access to Azure data lake ---------------------------------------------
  token <- AzureAuth::get_azure_token(token_path, "f99f92d3-7e2b-4820-858a-b629fad639e0", "b684d32b-6c1e-4538-af61-d19a1921ec80",
                                      password="RGG7Q~v4iXb4btVT47_pMkG0e8Sn_gDwK3DOw", auth_type="client_credentials")


  ad_endp_tok <- AzureStor::storage_endpoint(main_path, token=token)

  cont <- AzureStor::storage_container(ad_endp_tok, code)

  files <- AzureStor::list_storage_files(cont)


  fs <- AzureStor::adls_filesystem(paste(main_path, "/", code, sep = ""), token=token)

  # 1.1 Set up working directory & folders -------------------------------------

  dir.create(paste("./", code, "/", sep = ""))

  folders <-
  list_adls_files(fs, "/") %>%
    as_data_frame() %>%
    pull(name)


  for(i in 1:length(folders)) {

    AzureStor::multidownload_adls_file(fs, src = paste("/", folders[i], "/*.*", sep = ""), dest = paste("./", code, "/", folders[i], "/", sep = ""), overwrite = TRUE)

  }



#   AzureStor::multidownload_adls_file(fs, src = "/SalesTransactions/*.*", dest = paste("./", code, "/SalesTransactions/", sep = ""), overwrite = TRUE)
#
#   AzureStor::multidownload_adls_file(fs, src = "/*/*.*", dest = paste("./", code, "/SalesTransactions/", sep = ""), overwrite = TRUE)
#
# ?list_adls_files()
