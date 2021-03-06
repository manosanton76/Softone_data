% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/forecast_top_products.R
\name{forecast_top_products}
\alias{forecast_top_products}
\title{Apply monthly forecasting in the top products}
\usage{
forecast_top_products(code, datalake = 1)
}
\arguments{
\item{code}{Soft1 installation code}

\item{datalake}{Which Azure data lake to use (0 for production)}
}
\description{
It produces monthly forecasts the top products (in sales last 2 years)
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

forecast_top_products(code = "01100201021801", datalake = 1)

}

}
