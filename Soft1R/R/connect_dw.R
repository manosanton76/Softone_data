


#' Connect to Soft1 Data warehouse
#'
#' @description
#' Create a connection to Soft1 Data Warehouse DB, hosted in analytics.softone.gr
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
#' - The default inputs will create a connection to the Data warehouse DB (Analytics) with full rights
#'
#' @examples
#' \dontrun{
#' library(RODBC)
#'
#' Installations <- sqlQuery(connect_dw(), "Select TOP (100) * from [00_DM_Installations]")
#' }
#'
#' @export

connect_dw <- function(conn = conn, Driver = ifelse(base::Sys.info()['sysname'][[1]] == "Linux", "{ODBC Driver 17 for SQL Server}", "SQL Server"), Server = "analytics.softone.gr", Database = "ANALYTICS", Uid = "analytics", Pwd = "idlyKQcUWP%6&$23tn" ) {
  conn <- RODBC::odbcDriverConnect(paste("Driver=", Driver, "; Server=", Server, "; Database=", Database, "; Uid=", Uid, "; Pwd=", Pwd, sep = ""))

  return(conn)
}
