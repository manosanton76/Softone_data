% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/forecast_receipts.R
\name{forecast_receipts}
\alias{forecast_receipts}
\title{Apply forecasting in the receipts dataset}
\usage{
forecast_receipts(code, datalake = 1)
}
\arguments{
\item{code}{Soft1 installation code}

\item{datalake}{Which Azure data lake to use (0 for production)}
}
\description{
It produces yearly|monthly|weekly|daily forecasts, with & without anomaly detection
on the receipts dataset of the installation
}
\details{
\itemize{
\item Must provide a valid Soft1 installation code i.e. must have data in azure data lake
\item Use 0 on datalake, for storing results on the production Azure data lake.
Use 1 (default) for the dev Azure data lake
}
}
\examples{
\dontrun{

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

forecast_receipts(code = "01100201021801", datalake = 1)

}

}
