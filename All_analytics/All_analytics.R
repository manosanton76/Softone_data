
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
library(AzureStor)


# Define the blob storage
cont <- blob_container(
  "https://softonebilogs.blob.core.windows.net/softonebilogs",
  sas="sp=racwli&st=2022-04-01T13:42:50Z&se=2022-12-31T22:42:50Z&spr=https&sv=2020-08-04&sr=c&sig=mIpQrdjrjRmaOeWxR8j24k%2BVtH1JjajPk5CtOi6s9Cs%3D")

code <- "-"

# Function for sending email on failure
fail_email_general <- function(msg) {

  compose_email(
    body = md(glue::glue(
      "Softone analytics service")),
    footer = md(glue::glue("Email sent on {Sys.time()}."))
  ) %>%
    smtp_send(
      to = c("eam@softone.gr", "pgi@softone.gr", "dgp@softone.gr"),
      from = c("Softone Analytics Service" = "bi@softone.gr"),
      subject = msg,
      credentials = creds_file("./Logs/sendgrid_creds")
    )

}


tryCatch(
  expr = {


    # log_appender(appender_file(file = "./Logs/log.csv"))

    time_now <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

    # log_info('Script starting up...')

    # Read/Process jobs ------------------------------------------------------------
    jobs_original <-
      as.data.frame(readLines('https://glass.s1cloud.net/powerbi?listtype=1'))

    memfree <- as.numeric(system("awk '/MemFree/ {print $2}' /proc/meminfo", intern=TRUE))

    # log_info('There are {memfree} RAM available & {nrow(jobs_original)} jobs to run')

    cores <- parallel::detectCores()

    max_cpu <- 100 - (100/cores)

    cpu <-  as.numeric(system("top -b -n2 | grep 'Cpu(s)'|tail -n 1 | awk '{print $2 + $4}'", intern = TRUE))

    # 1. Nrows > 0 & memfree > 12 GB & cpu <= max_cpu ------------------------------
    if (nrow(jobs_original) > 0 & memfree > 12000000 & cpu <= max_cpu) {


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
      library(prophet)


      options(scipen = 999)

      dir.create("./Logs", showWarnings = FALSE)

      write.table(x = paste("Memory:", memfree, " - CPU:", cpu, " - Jobs:", nrow(jobs_original), sep = ""),
                  file = paste0("./Logs/", time_now, "_jobs.txt", sep = ""),
                  col.names = FALSE)

      # Upload files
      # upload_blob(cont, paste0("./Logs/", time_now, "_jobs.txt", sep = ""))

      jobs_original <-
        jobs_original %>%
        separate(col = 1, into = c("ajobid", "checkid", "serialno", "jobstatus", "dev"), sep = ";") %>%
        slice(1)


      code <- jobs_original$serialno


      jobs <-
        jobs_original %>%
        mutate(analytics = 1) %>%
        bind_rows(jobs_original %>% mutate(analytics = 2)) %>%
        bind_rows(jobs_original %>% mutate(analytics = 3)) %>%
        bind_rows(jobs_original %>% mutate(analytics = 4)) %>%
        # bind_rows(jobs_original %>% mutate(analytics = 5)) %>%
        bind_rows(jobs_original %>% mutate(analytics = 6)) %>%
        bind_rows(jobs_original %>% mutate(analytics = 7)) %>%
        arrange(ajobid, analytics) %>%
        tibble::rowid_to_column("id")

      job_starttime <- Sys.time()

      jobs_start <-
        jobs_original %>%
        select("ajobid", "checkid", "serialno") %>%
        mutate(start = job_starttime,
               finish = "")

      write.csv2(jobs_start, file = paste0("./Logs/", time_now, "_", jobs_original$serialno , "_main.csv", sep = ""), row.names = FALSE)

      # Upload files
      upload_blob(cont, paste0("./Logs/", time_now, "_", jobs_original$serialno , "_main.csv", sep = ""))

      # log_info('Start running job for installation: {jobs_original$serialno}')

      # Mark installations as Locked -------------------------------------------------
      for (i in 1:nrow(jobs)) {
        httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', jobs[i, "ajobid"], '&checkid=', jobs[i, "checkid"], '&jobstatus=3', sep = ""))
      }

      # Function for sending email on failure
      fail_email <- function(msg) {

        compose_email(
          body = md(glue::glue(
            "Softone analytics service")),
          footer = md(glue::glue("Email sent on {Sys.time()}."))
        ) %>%
          smtp_send(
            to = c("eam@softone.gr", "pgi@softone.gr", "dgp@softone.gr"),
            from = c("Softone Analytics Service" = "bi@softone.gr"),
            subject = paste(jobs_original$serialno, msg),
            credentials = creds_file("./Logs/sendgrid_creds")
          )

      }


      f <- list()

      # Run Analytics microservices ------------------------------------------------
      for (i in 1:nrow(jobs)) {

        # log_appender(appender_file(file = "./Logs/log.csv"))

        if (jobs[i, "analytics"] == 1) {

          tryCatch(
            expr = {
              forecast_sales(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
            },
            error = function(e){
              f[1] <<- "forecast_sales Failed!!!!"
              print("forecast_sales Failed!!!!")
              fail_email(": forecast_sales Failed!!!!")
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
              f[2] <<- "forecast_receipts Failed!!!!"
              print("forecast_receipts Failed!!!!")
              fail_email(": forecast_receipts Failed!!!!")
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
              f[3] <<- "cluster_customers Failed!!!!"
              print("cluster_customers Failed!!!!")
              fail_email(": cluster_customers Failed!!!!")
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
              f[4] <<- "cluster_items Failed!!!!"
              print("cluster_items Failed!!!!")
              fail_email(": cluster_items Failed!!!!")
              gc()
            },
            finally = {
              gc()

            }
          )


        }  else if (jobs[i, "analytics"]  == 5) {

          tryCatch(
            expr = {
              forecast_top_products(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
            },
            error = function(e){
              f[5] <<- "forecast_top_products Failed!!!!"
              print("forecast_top_products Failed!!!!")
              fail_email(": forecast_top_products Failed!!!!")
              gc()
            },
            finally = {
              gc()

            }
          )



        }  else if (jobs[i, "analytics"]  == 6) {

          tryCatch(
            expr = {
              association(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
            },
            error = function(e){
              f[6] <<- "association Failed!!!!"
              print("association Failed!!!!")
              fail_email(": association Failed!!!!")
              gc()
            },
            finally = {
              gc()

            }
          )


        } else if (jobs[i, "analytics"]  == 7) {

          tryCatch(
            expr = {
              forecast_top_products_prophet(code = jobs[i, "serialno"], datalake = as.numeric(jobs[i, "dev"]))
            },
            error = function(e){
              f[6] <<- "forecast_top_products_prophet Failed!!!!"
              print("forecast_top_products_prophet Failed!!!!")
              fail_email(": forecast_top_products_prophet Failed!!!!")
              gc()
            },
            finally = {
              gc()

            }
          )


        }



        print(i)
      }

      # Create Logs ------------------------------------------------------------------

      jobs_start <-
        jobs_original %>%
        select("ajobid", "checkid", "serialno") %>%
        mutate(start = job_starttime,
               finish = Sys.time(),
               Info = paste0((unlist(f)), collapse = ","))

      dir.create("./Logs", showWarnings = FALSE)

      write.csv2(jobs_start, file = paste0("./Logs/", time_now, "_jobs.csv", sep = ""), row.names = FALSE)

      # Upload files
      upload_blob(cont, paste0("./Logs/", time_now, "_jobs.csv", sep = ""))


      write.csv2(jobs_start, file = paste0("./Logs/", time_now, "_", jobs_original$serialno , "_main.csv", sep = ""), row.names = FALSE)

      # Upload files
      upload_blob(cont, paste0("./Logs/", time_now, "_", jobs_original$serialno , "_main.csv", sep = ""))

      # Update S1cloud jobs ----------------------------------------------------------

      # Mark succesfull installations as complete
      if (jobs_start$Info == "") {
        httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', jobs_original$ajobid, '&checkid=', jobs_original$checkid, '&jobstatus=4', sep = ""))
      } else {
        httr::POST(paste('https://glass.s1cloud.net/powerbi?jobid=', jobs_original$ajobid, '&checkid=', jobs_original$checkid, '&jobstatus=98', sep = ""))
      }


      # Sent e-mail ------------------------------------------------------------------

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
        add_attachment(file = paste0("./Logs/", time_now, "_", jobs_original$serialno , "_main.csv", sep = "")) %>%
        smtp_send(
          to = c("eam@softone.gr", "pgi@softone.gr", "dgp@softone.gr"),
          from = c("Softone Analytics Service" = "bi@softone.gr"),
          subject = paste("Softone analytics service (", first(jobs_original$serialno), ")"),
          credentials = creds_file("./Logs/sendgrid_creds")
        )

      # 2. Nrows > 0 & (memfree < 12000000 | cpu > max_cpu) -----------------------------
    }  else if (nrow(jobs_original) > 0 & (memfree < 12000000 | cpu > max_cpu)) {

      # log_warn('Failed to run jobs. Only {memfree} available RAM')

      filelist = list.files(path = './Logs', pattern = "*.txt", full.names = TRUE)

      #assuming tab separated values with a header
      datalist = lapply(filelist[(length(filelist)-3):length(filelist)], function(x)read.table(x))

      #assuming the same header/columns for all files
      data <-
        do.call("rbind", datalist) %>%
        select(V2) %>%
        separate(V2, c("ram", "cpu", "jobs"), sep = "([-])") %>%
        mutate(ram = parse_number(ram),
               cpu = parse_number(cpu),
               jobs = parse_number(jobs))


      filelist = last(list.files(path = './Logs', pattern = "*._jobs.csv", full.names = TRUE))

      t <-
        parse_datetime(
          paste(str_sub(filelist, start = 8, end = 11), "-",
                str_sub(filelist, start = 13, end = 14), "-",
                str_sub(filelist, start = 16, end = 17), " ",
                str_sub(filelist, start = 19, end = 20), ":",
                str_sub(filelist, start = 22, end = 23),
                sep = ""))


      now <- as.POSIXlt(Sys.time(), tz = "UTC")

      diff <- as.numeric(difftime(now, t, units="mins"))

      if (diff > 20 & (sum(data$ram < 12000000) == 4 | mean(data$cpu) > max_cpu )) {

        # Get a nicely formatted date/time string
        date_time <- add_readable_time()

        email <-
          compose_email(
            body = md(glue::glue(
              # filelist)),
              "Softone analytics service")),
            footer = md(glue::glue("Email sent on {date_time}."))
          )


        # Sending email by SMTP using a credentials file
        email %>%
          smtp_send(
            to = c("eam@softone.gr", "pgi@softone.gr", "dgp@softone.gr"),
            from = c("Softone Analytics Service" = "bi@softone.gr"),
            subject = paste("CPU - RAM Alert!!!!!! - (jobs > 0) - diff:", round(diff,1)),
            credentials = creds_file("./Logs/sendgrid_creds")
          )

        Soft1R::notify(paste("CPU - RAM Alert!!!!!! - (jobs > 0) - diff:", round(diff,1)))

      }

      time_now <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

      dir.create("./Logs", showWarnings = FALSE)

      write.table(x = paste("Memory:", memfree, " - CPU:", cpu, " - Jobs:", nrow(jobs_original), sep = ""),
                  file = paste0("./Logs/", time_now, "_jobs.txt", sep = ""),
                  col.names = FALSE)

      # Upload files
      # upload_blob(cont, paste0("./Logs/", time_now, "_jobs.txt", sep = ""))

      gc()


      # 3. Nrows = 0 & (memfree < 12000000 | cpu > max_cpu) ---------------------------
    }  else if (nrow(jobs_original) == 0 & (memfree < 12000000 | cpu > max_cpu)) {

      # log_warn('Only {memfree} available RAM')

      filelist = list.files(path = './Logs', pattern = "*.txt", full.names = TRUE)

      #assuming tab separated values with a header
      datalist = lapply(filelist[(length(filelist)-3):length(filelist)], function(x)read.table(x))

      #assuming the same header/columns for all files
      data <-
        do.call("rbind", datalist) %>%
        select(V2) %>%
        separate(V2, c("ram", "cpu", "jobs"), sep = "([-])") %>%
        mutate(ram = parse_number(ram),
               cpu = parse_number(cpu),
               jobs = parse_number(jobs))


      filelist = last(list.files(path = './Logs', pattern = "*._jobs.csv", full.names = TRUE))

      t <-
        parse_datetime(
          paste(str_sub(filelist, start = 8, end = 11), "-",
                str_sub(filelist, start = 13, end = 14), "-",
                str_sub(filelist, start = 16, end = 17), " ",
                str_sub(filelist, start = 19, end = 20), ":",
                str_sub(filelist, start = 22, end = 23),
                sep = ""))


      now <- as.POSIXlt(Sys.time(), tz = "UTC")

      diff <- as.numeric(difftime(now, t, units="mins"))

      if (diff > 20 & (sum(data$ram < 12000000) == 4 | mean(data$cpu) > max_cpu )) {

        # Get a nicely formatted date/time string
        date_time <- add_readable_time()


        email <-
          compose_email(
            body = md(glue::glue(
              # filelist)),
              "Softone analytics service")),
            footer = md(glue::glue("Email sent on {date_time}."))
          )


        # Sending email by SMTP using a credentials file
        email %>%
          smtp_send(
            to = c("eam@softone.gr", "pgi@softone.gr", "dgp@softone.gr"),
            from = c("Softone Analytics Service" = "bi@softone.gr"),
            subject = paste("CPU - RAM Alert!!!!!! - (jobs = 0) - diff:", round(diff,1)),
            credentials = creds_file("./Logs/sendgrid_creds")
          )

        Soft1R::notify(paste("CPU - RAM Alert!!!!!! - (jobs = 0) - diff:", round(diff,1)))

      }

      time_now <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

      dir.create("./Logs", showWarnings = FALSE)

      write.table(x = paste("Memory:", memfree, " - CPU:", cpu, " - Jobs:", nrow(jobs_original), sep = ""),
                  file = paste0("./Logs/", time_now, "_jobs.txt", sep = ""),
                  col.names = FALSE)

      # Upload files
      # upload_blob(cont, paste0("./Logs/", time_now, "_jobs.txt", sep = ""))

      gc()


      # 4. Nrows = 0 & memfree > 12000000 & cpu < max_cpu) ------------------------
    }  else {

      time_now <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")

      dir.create("./Logs", showWarnings = FALSE)

      write.table(x = paste("Memory:", memfree, " - CPU:", cpu, " - Jobs:", nrow(jobs_original), sep = ""),
                  file = paste0("./Logs/", time_now, "_jobs.txt", sep = ""),
                  col.names = FALSE)


      # Upload files
      # upload_blob(cont, paste0("./Logs/", time_now, "_jobs.txt", sep = ""))

      gc()

    }

    # log_info('Finished Running Script')

  },

  error = function(e){
    fail_email_general(paste("Proccess failed for ", code))
    Soft1R::notify(paste("Proccess failed for ", code))
    gc()

  },
  finally = {
    gc()
  }
)

rm(list = ls())
