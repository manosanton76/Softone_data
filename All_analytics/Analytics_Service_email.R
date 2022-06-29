

library(tidyverse)
library(blastula)


# main_path <- "/mnt/c/Users/eam/OneDrive - Softone Technologies S.A/Business_Analytics/00_Analytics_Projects/12_Production/"
main_path <- "/home/analytics/"

rmarkdown::render(paste(main_path, 'docker/docker_all_analytics/Analytics_Service.Rmd', sep = ""))

email <-
  compose_email(
    body = md(glue::glue(
      "Softone analytics service")),
    footer = md(glue::glue("Email sent on {Sys.time()}."))
  )


# Sending email by SMTP using a credentials file
email %>%
  add_attachment(file = paste(main_path, 'docker/docker_all_analytics/Analytics_Service.html', sep = "")) %>%
  smtp_send(
    to = c("eam@softone.gr", "pgi@softone.gr", "apn@softone.gr", "dgp@softone.gr"),
    from = c("Softone Analytics Service" = "bi@softone.gr"),
    subject = "Daily usage report for Softone BI predictive analytics service",
    credentials = creds_file(paste(main_path, "Logs/sendgrid_creds", sep = ""))
  )
