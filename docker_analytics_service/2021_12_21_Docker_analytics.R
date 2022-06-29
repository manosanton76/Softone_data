
library(httr)
library(RCurl)
library(tidyverse)
library(lubridate)
library(foreach)
library(doParallel)
library(tictoc)
library(Soft1R)
library(blastula)
library(readr)
library(logger)
library(glue)

# 1. Data Input ----------------------------------------------------------------
# serial <- "01142463802517"
# checkid <- "3796164566156386153"
# ajobid <- "900"

tt <- Sys.getenv(c("serial", "checkid", "ajobid"))


if (exists("serial") == FALSE) {

  serial <- tt[["serial"]]

}

if (exists("checkid") == FALSE) {

  checkid <- tt[["checkid"]]

}

if (exists("ajobid") == FALSE) {

  ajobid <- tt[["ajobid"]]

}


jobs <- structure(list(id = 1:6,
                       ajobid = c(ajobid, ajobid, ajobid, ajobid, ajobid, ajobid),
                       checkid = c(checkid, checkid, checkid, checkid, checkid, checkid),
                       serialno = c(serial, serial, serial, serial, serial, serial),
                       jobstatus = c("2", "2", "2", "2", "2", "2"),
                       dev = c("0", "0", "0", "0", "0", "0"),
                       analytics = c(1, 2, 3, 4, 5, 6)),
                  row.names = c(NA, -6L), class = "data.frame")

# 1.1 Logs ---------------------------------------------------------------------

dir.create("./Logs", showWarnings = FALSE)

time_now <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

job_starttime <- Sys.time()

jobs_start <-
  jobs %>%
  select("ajobid", "checkid", "serialno") %>%
  mutate(start = job_starttime,
         finish = "")

write.csv2(jobs_start, file = paste0("./Logs/", time_now, "_", serial , "_main.csv", sep = ""), row.names = FALSE)


# Function for sending email on failure
fail_email <- function(msg) {

  compose_email(
    body = md(glue::glue(
      "Softone analytics service")),
    footer = md(glue::glue("Email sent on {Sys.time()}."))
  ) %>%
    smtp_send(
      to = c("eam@softone.gr"),
      from = "manosantonioutest@gmail.com",
      subject = paste(serial, msg),
      credentials = creds_file("gmail_creds")
    )

}

# 2. Mark installation as Locked -------------------------------------------------
httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', ajobid, '&checkid=', checkid, '&jobstatus=3', sep = ""))

f <- list()


# 3. Run Analytics ----------------------------------------------------------------
x <- foreach(i=1:nrow(jobs), .errorhandling = 'remove', .combine = "cbind") %do% {


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
  library(tictoc)
  library(recommenderlab)
  library(arules)
  library(arulesViz)
  library(logger)
  library(glue)


  if (jobs[i, "analytics"] == 1) {

    tryCatch(
      expr = {
        forecast_sales(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
      },
      error = function(e){
        print("Task 1 Failed!!!!")
        fail_email(": Task 1 Failed!!!!")
        f[1] <<- ": Task 1 Failed!!!!"
        gc()
      },
      finally = {
        print("Finish 1")
        gc()

      }
    )


  } else if (jobs[i, "analytics"]  == 2) {

    tryCatch(
      expr = {
        forecast_receipts(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
      },
      error = function(e){
        print("Task 2 Failed!!!!")
        fail_email(": Task 2 Failed!!!!")
        f[2] <<- ": Task 2 Failed!!!!"
        gc()
      },
      finally = {
        print("Finish 2")
        gc()

      }
    )


  } else if (jobs[i, "analytics"]  == 3) {

    tryCatch(
      expr = {
        cluster_customers(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
      },
      error = function(e){
        print("Task 3 Failed!!!!")
        fail_email(": Task 3 Failed!!!!")
        f[3] <<- ": Task 3 Failed!!!!"
        gc()
      },
      finally = {
        print("Finish 3")
        gc()

      }
    )


  }  else if (jobs[i, "analytics"]  == 4) {

    tryCatch(
      expr = {
        cluster_items(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
      },
      error = function(e){
        print("Task 4 Failed!!!!")
        fail_email(": Task 4 Failed!!!!")
        f[4] <<- ": Task 4 Failed!!!!"
        gc()
      },
      finally = {
        print("Finish 4")
        gc()

      }
    )


  }  else if (jobs[i, "analytics"]  == 5) {

    tryCatch(
      expr = {
        forecast_top_products(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
      },
      error = function(e){
        print("Task 5 Failed!!!!")
        fail_email(": Task 5 Failed!!!!")
        f[5] <<- ": Task 5 Failed!!!!"
        gc()
      },
      finally = {
        print("Finish 5")
        gc()

      }
    )



  }  else if (jobs[i, "analytics"]  == 6) {

    tryCatch(
      expr = {
        association(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
      },
      error = function(e){
        print("Task 6 Failed!!!!")
        fail_email(": Task 6 Failed!!!!")
        f[6] <<- ": Task 6 Failed!!!!"
        gc()
      },
      finally = {
        print("Finish 6")
        gc()

      }
    )


  }


  print(i)
}



# 4. Create Logs ------------------------------------------------------------------
test <-
  x %>%
  t() %>%
  as.data.frame() %>%
  bind_cols(run = 1)

jobs_final <-
  jobs %>%
  full_join(test, by = c("id" = "V1")) %>%
  replace_na(list(run = 0))

write.csv2(jobs_final, file = paste0("./Logs/", time_now, "_jobs.csv", sep = ""), row.names = FALSE)


jobs_new <-
  jobs_final %>%
  group_by(ajobid, checkid, serialno, jobstatus, dev) %>%
  summarise(run = mean(run))


jobs_start <-
  jobs %>%
  head(1) %>%
  select("ajobid", "checkid", "serialno") %>%
  mutate(start = job_starttime,
         finish = Sys.time(),
         Info = paste0((unlist(f)), collapse = ","))

write.csv2(jobs_start, file = paste0("./Logs/", time_now, "_", serial , "_main.csv", sep = ""), row.names = FALSE)


# 5. Update S1cloud jobs ----------------------------------------------------------

# Mark succesfull installations as complete
if (nrow(filter(jobs_new, run == 1)) >= 1) {
    httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', ajobid, '&checkid=', checkid, '&jobstatus=4', sep = ""))
}

# Mark unsuccesful installations as failed
if (nrow(filter(jobs_new, run < 1)) >= 1) {
    httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', ajobid, '&checkid=', checkid, '&jobstatus=98', sep = ""))
}



# 6. Sent e-mail ------------------------------------------------------------------

# Get a nicely formatted date/time string
date_time <- add_readable_time()

email <-
  compose_email(
    body = md(glue::glue(
      "Softone analytics service")),
    footer = md(glue::glue("Email sent on {date_time}."))
  )


# Sending email by SMTP using a credentials file
email %>%
  add_attachment(file = paste0("./Logs/", time_now, "_", serial , "_main.csv", sep = "")) %>%
  smtp_send(
    to = c("eam@softone.gr", "pgi@softone.gr"),
    from = "manosantonioutest@gmail.com",
    subject = paste("Softone analytics service (", serial, ")"),
    credentials = creds_file("gmail_creds")
  )


