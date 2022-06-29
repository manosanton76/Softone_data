


#' Clustering Customers in a Soft1 installation
#'
#' @description
#' cluster_customers() returns a dataset combining 5 separate datasets with clustering
#' results (for k = 2,3,4,5,6) using cluster_s1(), after passing a valid Soft1
#' installation code
#'
#' @param code Soft1 installation code
#' @param datalake Which Azure data lake to use (0 for production)
#'
#' @details
#'
#' - Must provide a dataframe with a categorical variable (first variable) and 2 or more continuous variables
#' - The provided dataframe cannot include missing values
#' - Use 0 on datalake, for storing results on the production Azure data lake.
#' Use 1 (default) for the dev Azure data lake
#' - Only the top 200.000 customers (first by number of transactions and then by sales) are included
#'
#' @examples
#' \dontrun{
#' library(tidyverse)
#' library(lubridate)
#' library(cluster)
#' library(sweep)
#' library(Soft1R)
#' library(AzureAuth)
#' library(AzureStor)
#' library(data.table)
#'
#' cluster_customers(code = "01100201021801", datalake = 1)
#' }
#'
#' @export

cluster_customers <- function(code, datalake = 1) {

  ptm <- proc.time()


  if (datalake == 0 ) {
    token_path <- "https://s1datalakeprod01.blob.core.windows.net"
    main_path <- "https://s1datalakeprod01.dfs.core.windows.net"
  } else {
    token_path <- "https://s1azdatalake01.blob.core.windows.net"
    main_path <- "https://s1azdatalake01.dfs.core.windows.net"
  }


  # 1. Get Access to Azure data lake ---------------------------------------------
  token <- AzureAuth::get_azure_token(token_path,  auth_type="client_credentials")


  ad_endp_tok <- AzureStor::storage_endpoint(main_path, token=token)

  cont <- AzureStor::storage_container(ad_endp_tok, code)

  files <- AzureStor::list_storage_files(cont)


  fs <- AzureStor::adls_filesystem(paste(main_path, "/", code, sep = ""), token=token)

  # 1.1 Set up working directory & folders -------------------------------------
  temp_path <- "/cluster_customers"
  dir.create(paste(tempdir(), "/cluster_customers/", sep = ""))


  dir.create(paste(tempdir(), temp_path, "/upload/", sep = ""))
  dir.create(paste(tempdir(), temp_path, "/data/", sep = ""))


  # 1.2 Upload Empty files -----------------------------------------------------


  # 1.2.1 Create empty detail data objects -------------------------------------

  # Customer Segmentation
  data_types <-
    structure(list(custid = "string",
                   mediansales = "float",
                   totalsales = "float",
                   transactions = "float",
                   recency = "float",
                   segment = "string",
                   clusteringnumsegments = "string",
                   pc1 = "float",
                   pc2 = "float",
                   datetimecreated = "datetime",
                   `Custid-UniqueKey` = "string"), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, -1L))


  readr::write_csv2(data_types, path = paste(tempdir(), temp_path, "/upload", "/CustomerSegmentationAllCompanies.csv", sep = ""), na = "", )

  data_types2 <-
    structure(list(custid = "string",
                   mediansales = "float",
                   totalsales = "float",
                   transactions = "float",
                   recency = "float",
                   segment = "string",
                   clusteringnumsegments = "string",
                   pc1 = "float",
                   pc2 = "float",
                   datetimecreated = "datetime",
                   cmpcode = "integer",
                   `Custid-UniqueKey` = "string"), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, -1L))

  readr::write_csv2(data_types2, path = paste(tempdir(), temp_path, "/upload/CustomerSegmentationPerCompany.csv", sep = ""), na = "")

  # 1.2.2 Create empty zip files -----------------------------------------------

  dir.create(paste(tempdir(), temp_path, "/upload/CustomerSegmentationAllCompanies", sep = ""))
  dir.create(paste(tempdir(), temp_path, "/upload/CustomerSegmentationPerCompany", sep = ""))


  system(paste("zip -9 -y -j -q ", tempdir(), temp_path, "/upload/CustomerSegmentationAllCompanies/CustomerSegmentationAllCompanies.zip", " ", tempdir(), temp_path, "/upload/CustomerSegmentationAllCompanies.csv", sep = ""))
  system(paste("zip -9 -y -j -q ", tempdir(), temp_path, "/upload/CustomerSegmentationPerCompany/CustomerSegmentationPerCompany.zip", " ", tempdir(), temp_path, "/upload/CustomerSegmentationPerCompany.csv", sep = ""))


  # 1.2.3 Upload empty zip files  ----------------------------------------------

  AzureStor::storage_multiupload(cont, paste(tempdir(), temp_path, "/upload/", "/CustomerSegmentationAllCompanies/*.zip", sep = ""), "CustomerSegmentationAllCompanies")  # uploading everything in a directory
  AzureStor::storage_multiupload(cont, paste(tempdir(), temp_path, "/upload/", "/CustomerSegmentationPerCompany/*.zip", sep = ""), "CustomerSegmentationPerCompany")  # uploading everything in a directory


  # 2. Download transactional datasets -------------------------------------------


  AzureStor::multidownload_adls_file(fs, src = "/SalesTransactions/*.*", dest = paste(tempdir(), temp_path, "/data/", sep = ""), overwrite = TRUE)

  # 3. Unzip & Read dataset in R -----------------------------------------------
  zipfiles <- list.files(path = paste(tempdir(), temp_path, "/data/", sep = ""), pattern = "*.zip", full.names = TRUE) # data frame of zip files in current directory


  for (i in seq_along(zipfiles)) {

    utils::unzip(zipfiles[i], exdir = paste(tempdir(), temp_path, "/data/csv", sep = ""))

  }

  # Delete empty csv files
  for (i in list.files(path = paste(tempdir(), temp_path, "/data/csv", sep = ""), pattern="*.csv", full.names = TRUE)){
    if (nrow(data.table::fread(i)) <= 1) {unlink(i)}
  }



  files = list.files(path = paste(tempdir(), temp_path, "/data/csv", sep = ""), pattern="*.csv", full.names = TRUE)



  vmtrstat <-
    data.table::rbindlist(lapply(files, data.table::fread, colClasses=list(character=1:ncol(data.table::fread(files[1]))), skip = 2)) %>%
    dplyr::as_tibble() %>%
    rename_with(~ names(data.table::fread(files[1]))) %>%
    filter(custid != "") %>%
    select(cmpcode, salesid, date, custid, itemid, salesval, salescost, salesqty1)


  vmtrstat <-
    vmtrstat %>%
    dplyr::mutate(
      date = with_tz(as_datetime(as.POSIXct(as_datetime(vmtrstat$date), tz="Europe/London")), tzone = "Europe/Athens"),
      date = as.Date(as.character(date))
    ) %>%
    dplyr::filter(date > max(date) - lubridate::years(3)) %>%
    dplyr::mutate(salesval = as.numeric(gsub(",", ".", salesval)),
                  salescost = as.numeric(gsub(",", ".", salescost)),
                  salesqty1 = as.numeric(gsub(",", ".", salesqty1)))


  top_customers <-
    vmtrstat %>%
    dplyr::group_by(custid) %>%
    dplyr::summarise(N = n(),
                     Sales = sum(salesval, na.rm = TRUE)) %>%
    dplyr::arrange(-N, -Sales) %>%
    head(200000) %>%
    dplyr::pull(custid)

  vmtrstat <-
    vmtrstat %>%
    dplyr::filter(custid %in% top_customers)


  max_date <- max(vmtrstat$date)

  # Customer segmentation all companies
  cluster_data_all <<-
    vmtrstat %>%
    dplyr::group_by(custid, salesid, date) %>%
    dplyr::summarise(
      sales = sum(salesval),
      len = length(itemid)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(custid) %>%
    dplyr::summarise(
      mediansales = stats::median(sales),
      totalsales = sum(sales),
      transactions = length(custid),
      recency = as.numeric(max_date - max(date))
    )


  # Customer segmentation per company
  cluster_data_per_company <<-
    vmtrstat %>%
    dplyr::group_by(cmpcode, custid, salesid, date) %>%
    dplyr::summarise(
      sales = sum(salesval),
      len = length(itemid)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(cmpcode, custid) %>%
    dplyr::summarise(
      mediansales = stats::median(sales),
      totalsales = sum(sales),
      transactions = length(custid),
      recency = as.numeric(max_date - max(date))
    )



  # 4. Segmentation all companies ----------------------------------------------

  # Customer segmentation
  cust_unique <-
    vmtrstat %>%
    dplyr::group_by(custid, cmpcode) %>%
    dplyr::do(utils::head(., 1)) %>%
    dplyr::mutate(`Custid-UniqueKey` = paste(cmpcode, custid, sep = "-")) %>%
    dplyr::ungroup() %>%
    dplyr::select(custid, `Custid-UniqueKey`)

  new <-
    Soft1R::cluster_s1() %>%
    dplyr::mutate(datetimecreated = paste(as.character(lubridate::format_ISO8601(Sys.time())), ".000Z", sep = "")) %>%
    dplyr::mutate_if(is.numeric, decimal_sep) %>%
    dplyr::mutate_if(is.factor, as.character) %>%
    dplyr::left_join(cust_unique)



  # 5. Segmentation for each individual company --------------------------------
  # companies <- unique(cluster_data_per_company$cmpcode)

  companies <-
    cluster_data_per_company %>%
    dplyr::group_by(cmpcode) %>%
    dplyr::count() %>%
    dplyr::filter(n >= 15) %>%
    dplyr::pull(cmpcode)

  l <- list()

  for (i in 1:length(companies)) {

    try(

      test <-
        Soft1R::cluster_s1(final_dataset = cluster_data_per_company %>% dplyr::filter(cmpcode == companies[i]) %>% as.data.frame() %>% dplyr::select(-cmpcode)) %>%
        dplyr::mutate(datetimecreated = paste(as.character(lubridate::format_ISO8601(Sys.time())), ".000Z", sep = "")) %>%
        dplyr::mutate(cmpcode = as.character(companies[i]),
                      `Custid-UniqueKey` = paste(cmpcode, custid, sep = "-"))

    )


    l[[companies[i]]] <- test

  }


  # Customer Segmentation
  new_per_company <-
    as.data.frame(do.call(rbind, l)) %>%
    dplyr::mutate_if(is.numeric, decimal_sep) %>%
    dplyr::mutate_if(is.factor, as.character)



  # 6. Update detail data objects ----------------------------------------------

  utils::write.table(new, file = paste(tempdir(), temp_path, "/upload", "/CustomerSegmentationAllCompanies.csv", sep = ""), sep = ";", na = "", quote=c(1, 7, 11), row.names = FALSE, col.names = FALSE, append = TRUE)
  utils::write.table(new_per_company, file = paste(tempdir(), temp_path, "/upload/CustomerSegmentationPerCompany.csv", sep = ""), sep = ";", na = "", quote=c(1, 7, 12), row.names = FALSE, col.names = FALSE, append = TRUE)

  # 7. Update the zip files ----------------------------------------------------

  system(paste("zip -9 -y -j -q ", tempdir(), temp_path, "/upload/CustomerSegmentationAllCompanies/CustomerSegmentationAllCompanies.zip", " ", tempdir(), temp_path, "/upload/CustomerSegmentationAllCompanies.csv", sep = ""))
  system(paste("zip -9 -y -j -q ", tempdir(), temp_path, "/upload/CustomerSegmentationPerCompany/CustomerSegmentationPerCompany.zip", " ", tempdir(), temp_path, "/upload/CustomerSegmentationPerCompany.csv", sep = ""))


  # 8. Upload updated zip files ------------------------------------------------

  AzureStor::storage_multiupload(cont, paste(tempdir(), temp_path, "/upload/", "/CustomerSegmentationAllCompanies/*.zip", sep = ""), "CustomerSegmentationAllCompanies")  # uploading everything in a directory
  AzureStor::storage_multiupload(cont, paste(tempdir(), temp_path, "/upload/", "/CustomerSegmentationPerCompany/*.zip", sep = ""), "CustomerSegmentationPerCompany")  # uploading everything in a directory

  # 9. Create log files --------------------------------------------------------

  time_n <- proc.time() - ptm

  time_now2 <- as.character(format(Sys.time(), "%Y_%m_%d_%H_%M_%S"))

  segmentation <-
    new %>%
    group_by(clusteringnumsegments, segment) %>%
    summarise(N = n()) %>%
    mutate(type = "customer") %>%
    # bind_rows(
    #   new_items %>%
    #     group_by(clusteringnumsegments, segment) %>%
    #     summarise(N = n()) %>%
    #     mutate(type = "item")
    # ) %>%
    mutate(code = code,
           date = time_now2,
           vmtrstat_rows = nrow(vmtrstat),
           customers = length(unique(vmtrstat$custid)),
           products = length(unique(vmtrstat$itemid)),
           duration =   round(as.numeric(time_n[3]), 1))


  dir.create("./Logs", showWarnings = FALSE)

  write.csv2(segmentation, file = paste0("./Logs/", time_now2, "_Segmentation_Customers.csv", sep = ""), row.names = FALSE)

  # Define the blob storage
  cont <- AzureStor::blob_container(
    "https://softonebilogs.blob.core.windows.net/softonebilogs",
    sas="sp=racwli&st=2022-04-01T13:42:50Z&se=2022-12-31T22:42:50Z&spr=https&sv=2020-08-04&sr=c&sig=mIpQrdjrjRmaOeWxR8j24k%2BVtH1JjajPk5CtOi6s9Cs%3D")


  AzureStor::upload_blob(cont, paste0("./Logs/", time_now2, "_Segmentation_Customers.csv", sep = ""))

  # 10.  Clean disk ------------------------------------------------------------
  unlink(paste(tempdir(), temp_path, sep = ""), recursive = T)

}
