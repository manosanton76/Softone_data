% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cluster_main.R
\name{cluster_main}
\alias{cluster_main}
\title{Clustering in a Soft1 installation}
\usage{
cluster_main(code, datalake = 1)
}
\arguments{
\item{code}{Soft1 installation code}

\item{datalake}{Which Azure data lake to use (0 for production)}
}
\description{
cluster_main() returns a dataset combining 5 separate datasets with clustering
results (for k = 2,3,4,5,6) using cluster_s1(), after passing a valid Soft1
installation code
}
\details{
\itemize{
\item Must provide a dataframe with a categorical variable (first variable) and 2 or more continuous variables
\item The provided dataframe cannot include missing values
\item Use 0 on datalake, for storing results on the production Azure data lake.
Use 1 (default) for the dev Azure data lake
}
}
\examples{
\dontrun{
library(tidyverse)
library(lubridate)
library(cluster)
library(sweep)
library(Soft1R)
library(AzureAuth)
library(AzureStor)
library(data.table)

cluster_main(code = "01100201021801", datalake = 1)
}

}
