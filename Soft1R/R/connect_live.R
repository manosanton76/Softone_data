


#' Connect to Soft1 ERP database
#'
#' @description
#' Create a connection to Soft1 ERP transactional DB, hosted in Azure
#'
#' @param conn Specify the connection
#' @param Driver Specify the ODBC driver used for the connection
#' @param Server Specify the SQL Server of the DB
#' @param Database Specify the DB name
#' @param Uid Specify the user name
#' @param Pwd Specify the password
#'
#' @details
#'
#' - The default inputs will create a connection to the Soft1 DB with read rights
#'
#' @examples
#' \dontrun{
#'
#' library(RODBC)
#'
#' Installations <- sqlQuery(connect_live(), "Select TOP (100) * from [INST]")
#' }
#'
#' @export

connect_live <- function(conn = conn, Driver = ifelse(base::Sys.info()['sysname'][[1]] == "Linux", "{ODBC Driver 17 for SQL Server}", "SQL Server"), Server = "11100000223803.s1data.net", Database = "SOFTONE", Uid = "analytics", Pwd = "" ) {
  conn <- RODBC::odbcDriverConnect(paste("Driver=", Driver, "; Server=", Server, "; Database=", Database, "; Uid=", Uid, "; Pwd=", Pwd, sep = ""))

  return(conn)
}
