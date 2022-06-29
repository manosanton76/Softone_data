
library(httr)
library(RCurl)
library(tidyverse)
library(blastula)
library(parallel)
library(lubridate)
library(Soft1R)
library(readr)
library(parsedate)
library(glue)
library(foreach)
library(doParallel)
library(tictoc)
library(pushoverr)
library(AzureStor)


# Define the blob storage
cont <- blob_container(
  "https://softonebilogs.blob.core.windows.net/softonebilogs",


code <- "-"

# Function for sending email on failure
fail_email_general <- function(msg) {

  compose_email(
    body = md(glue::glue(
      "Softone analytics service")),
    footer = md(glue::glue("Email sent on {Sys.time()}."))
  ) %>%
    smtp_send(
      to = c("eam@softone.gr", "pgi@softone.gr"),
      from = c("Softone Analytics Service" = "eam@softone.gr"),
      subject = msg,
      credentials = creds_file("/home/sendgrid_creds")
    )

}


tryCatch(
  expr = {

    # 1. Create job objects --------------------------------------------------------
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

    jobs_original <- structure(list(ajobid = ajobid,
                                    checkid = checkid,
                                    serialno = serial,
                                    jobstatus = "2",
                                    dev = "0"),
                               class = "data.frame", row.names = c(NA, -1L))

    jobs <- structure(list(id = 1:6,
                           ajobid = c(ajobid, ajobid, ajobid, ajobid, ajobid, ajobid),
                           checkid = c(checkid, checkid, checkid, checkid, checkid, checkid),
                           serialno = c(serial, serial, serial, serial, serial, serial),
                           jobstatus = c("2", "2", "2", "2", "2", "2"),
                           dev = c("0", "0", "0", "0", "0", "0"),
                           analytics = c(1, 2, 3, 4, 6, 7)),
                      row.names = c(NA, -6L), class = "data.frame")

    code <- jobs_original$serialno


    # 2 Create Logs --------------------------------------------------------------

    dir.create(paste(tempdir(), "/Logs", sep = ""), showWarnings = FALSE)

    time_now <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

    job_starttime <- Sys.time()

    jobs_start <-
      jobs_original %>%
      select("ajobid", "checkid", "serialno") %>%
      mutate(start = job_starttime,
             finish = "")

    write.csv2(jobs_start, file = paste0(paste(tempdir(), "/Logs/", sep = ""), time_now, "_", serial , "_main.csv", sep = ""), row.names = FALSE)

    # Upload files
    upload_blob(cont, paste0(paste(tempdir(), "/Logs/", sep = ""), time_now, "_", serial , "_main.csv", sep = ""))


    # Function for sending email on failure
    fail_email <- function(msg) {

      compose_email(
        body = md(glue::glue(
          "Softone analytics service")),
        footer = md(glue::glue("Email sent on {Sys.time()}."))
      ) %>%
        smtp_send(
          to = c("eam@softone.gr"),
          from = "eam@softone.gr",
          subject = paste(serial, msg),
          credentials = creds_file("/home/sendgrid_creds")
        )

    }

    # 3. Mark installation as Locked -------------------------------------------------
    httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', ajobid, '&checkid=', checkid, '&jobstatus=3', sep = ""))

    f <- list()

    library(forecast)
    library(sweep)
    library(tidyquant)
    library(AzureAuth)
    library(AzureStor)
    library(data.table)
    library(anomalize)
    library(cluster)
    library(recommenderlab)
    library(arules)
    library(arulesViz)

    # 4. Run Analytics ----------------------------------------------------------------
    for (i in 1:nrow(jobs)) {


      if (jobs[i, "analytics"] == 1) {

        tryCatch(
          expr = {
            forecast_sales(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
          },
          error = function(e){
            f[1] <<- "Task 1 Failed!!!!"
            print("Finish 1 Failed!!!!")
            fail_email(": Task 1 Failed!!!!")
            gc()
          },
          finally = {
            gc()

          }
        )


      } else if (jobs[i, "analytics"]  == 2) {

        tryCatch(
          expr = {
            forecast_receipts(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
          },
          error = function(e){
            f[2] <<- "Task 2 Failed!!!!"
            print("Finish 2 Failed!!!!")
            fail_email(": Task 2 Failed!!!!")
            gc()
          },
          finally = {
            gc()

          }
        )


      } else if (jobs[i, "analytics"]  == 3) {

        tryCatch(
          expr = {
            cluster_customers(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
          },
          error = function(e){
            f[3] <<- "Task 3 Failed!!!!"
            print("Finish 3 Failed!!!!")
            fail_email(": Task 3 Failed!!!!")
            gc()
          },
          finally = {
            gc()

          }
        )


      }  else if (jobs[i, "analytics"]  == 4) {

        tryCatch(
          expr = {
            cluster_items(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
          },
          error = function(e){
            f[4] <<- "Task 4 Failed!!!!"
            print("Finish 4 Failed!!!!")
            fail_email(": Task 4 Failed!!!!")
            gc()
          },
          finally = {
            gc()

          }
        )


      }   else if (jobs[i, "analytics"]  == 6) {

        tryCatch(
          expr = {
            association(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
          },
          error = function(e){
            f[6] <<- "Task 6 Failed!!!!"
            print("Finish 6 Failed!!!!")
            fail_email(": Task 6 Failed!!!!")
            gc()
          },
          finally = {
            gc()

          }
        )


      }  else if (jobs[i, "analytics"]  == 7) {

        tryCatch(
          expr = {
            forecast_top_products_prophet(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
          },
          error = function(e){
            f[7] <<- "Task 7 Failed!!!!"
            print("Finish 7 Failed!!!!")
            fail_email(": Task 7 Failed!!!!")
            gc()
          },
          finally = {
            gc()

          }
        )



      }


      print(i)
    }



    # 5. Create Logs ------------------------------------------------------------------

    jobs_start <-
      jobs_original %>%
      select("ajobid", "checkid", "serialno") %>%
      mutate(start = job_starttime,
             finish = Sys.time(),
             Info = paste0((unlist(f)), collapse = ","))

    dir.create("./Logs", showWarnings = FALSE)

    write.csv2(jobs_start, file = paste0(paste(tempdir(), "/Logs/", sep = ""), time_now, "_jobs.csv", sep = ""), row.names = FALSE)

    # Upload files
    upload_blob(cont, paste0(paste(tempdir(), "/Logs/", sep = ""), time_now, "_jobs.csv", sep = ""))


    write.csv2(jobs_start, file = paste0(paste(tempdir(), "/Logs/", sep = ""), time_now, "_", jobs_original$serialno , "_main.csv", sep = ""), row.names = FALSE)

    # Upload files
    upload_blob(cont, paste0(paste(tempdir(), "/Logs/", sep = ""), time_now, "_", jobs_original$serialno , "_main.csv", sep = ""))



    # 6. Update S1cloud jobs ----------------------------------------------------------

    # Mark succesfull installations as complete
    if (jobs_start$Info == "") {
      httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', jobs_original$ajobid, '&checkid=', jobs_original$checkid, '&jobstatus=4', sep = ""))
    } else {
      httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', jobs_original$ajobid, '&checkid=', jobs_original$checkid, '&jobstatus=98', sep = ""))
    }


    # 7. Sent e-mail ------------------------------------------------------------------

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
      add_attachment(file = paste0(paste(tempdir(), "/Logs/", sep = ""), time_now, "_", jobs_original$serialno , "_main.csv", sep = "")) %>%
      smtp_send(
        to = c("eam@softone.gr", "pgi@softone.gr"),
        from = c("Softone Analytics Service" = "eam@softone.gr"),
        subject = paste("Softone analytics service (", first(jobs_original$serialno), ")"),
        credentials = creds_file("/home/sendgrid_creds")

      )

  },

  error = function(e){
    Soft1R::notify(paste("Proccess failed for ", code))
    fail_email_general(paste("Proccess failed for ", code))
    gc()

  },
  finally = {
    gc()
  }
)

rm(list = ls())

