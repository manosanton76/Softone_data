

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
# datalake <- 1

code <- "01102486514818" # Marketing demo
code <- "01142463802517" # Demo
code <- "01102467084017" # Nak shoes
code <- "01100201021801"
code <- "01102467074017" # Georgakopoulos
code <- "01100201021805"
code <- "01102328772812" # Marketing demo

code <- "01100253604408" # Large demo Pantolpolia
code <- "11100000223803" # Fani
code <- "211122050911" # Atlantis test main
code <- "01104243601420" # Goutzidis test
code <- "01104257114220" # Connect Line
code <- "01102467864217" # Nestle - Cyprus
code <- "01100234223907" # Blue NET


code <- "210257231512" # Atlantis test - empty
code <- "210231691112" # Atlantis test - empty
code <- "211137671512" # Atlantis test - empty
code <- "211140781612" # Atlantis test - empty

# Run jobs one by one ----------------------------------------------------------

forecast_sales(code = code, datalake = datalake)

forecast_receipts(code = code, datalake = datalake)

cluster_customers(code = code, datalake = datalake)

cluster_items(code = code, datalake = datalake)

forecast_top_products(code = code, datalake = datalake)

association(code = code, datalake = datalake)

# forecast_top_products_prophet(code = code, datalake = datalake)

# cluster_main(code = code, datalake = datalake)


# Run all jobs for 1 installation ----------------------------------------------

#setup parallel backend to use many processors
cores = detectCores()
cl <- makeCluster(cores - 1) #not to overload your computer
registerDoParallel(cl)

ttt <- c(1, 2, 3)

foreach(i=1:length(ttt)) %dopar% {


  library(Soft1R)
  library(tidyverse)
  library(lubridate)
  library(forecast)
  library(sweep)
  library(tidyquant)
  library(AzureAuth)
  library(AzureStor)
  library(data.table)
  library(parsedate)
  library(anomalize)
  library(cluster)


  if (ttt[i] == 1) {
    forecast_sales(code = code, datalake = datalake)
    # print(getwd())
  } else if (ttt[i] == 2) {
    forecast_receipts(code = code, datalake = datalake)

  } else if (ttt[i] == 3) {
    cluster_main(code = code, datalake = datalake)

  }

}



#stop cluster
stopCluster(cl)


# OLD --------------------------------------------------------------------------




#setup parallel backend to use many processors
cores = detectCores()
cl <- makeCluster(cores - 1) #not to overload your computer
registerDoParallel(cl)


con <- file(paste0(time_now, "_analytics.txt", sep = ""))

sink(con, append=TRUE)



foreach(i=1:nrow(jobs)) %dopar% {


  library(Soft1R)
  library(tidyverse)
  library(lubridate)
  library(forecast)
  library(sweep)
  library(tidyquant)
  library(AzureAuth)
  library(AzureStor)
  library(data.table)
  library(parsedate)
  library(anomalize)
  library(cluster)


  if (jobs[i, "analytics"] == 1) {
    forecast_sales(code = jobs[i, "licence"], datalake = as.numeric(jobs[i, "dev"]))
    # print(getwd())
  } else if (jobs[i, "analytics"]  == 2) {
    forecast_receipts(code = jobs[i, "licence"], datalake = as.numeric(jobs[i, "dev"]))

  } else if (jobs[i, "analytics"]  == 3) {
    cluster_main(code = jobs[i, "licence"], datalake = as.numeric(jobs[i, "dev"]))

  }

}



#stop cluster
stopCluster(cl)


toc()

sink()
