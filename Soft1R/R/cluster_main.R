


#' Clustering in a Soft1 installation
#'
#' @description
#' cluster_main() returns a dataset combining 5 separate datasets with clustering
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
#' cluster_main(code = "01100201021801", datalake = 1)
#' }
#'
#' @export

cluster_main <- function(code, datalake = 1) {

  ptm <- proc.time()


  if (datalake == 0 ) {
    token_path <- "https://s1datalakeprod01.blob.core.windows.net"
    main_path <- "https://s1datalakeprod01.dfs.core.windows.net"
  } else {
    token_path <- "https://s1azdatalake01.blob.core.windows.net"
    main_path <- "https://s1azdatalake01.dfs.core.windows.net"
  }


  # 1. Get Access to Azure data lake ---------------------------------------------
  token <- AzureAuth::get_azure_token(token_path, "f99f92d3-7e2b-4820-858a-b629fad639e0", "b684d32b-6c1e-4538-af61-d19a1921ec80",
                           password="RGG7Q~v4iXb4btVT47_pMkG0e8Sn_gDwK3DOw", auth_type="client_credentials")


  ad_endp_tok <- AzureStor::storage_endpoint(main_path, token=token)

  cont <- AzureStor::storage_container(ad_endp_tok, code)

  files <- AzureStor::list_storage_files(cont)


  fs <- AzureStor::adls_filesystem(paste(main_path, "/", code, sep = ""), token=token)

  # 1.1 Set up working directory & folders -------------------------------------

  tempid <- Soft1R::create_unique_ids(1)
  dir.create(paste(getwd(), '/Temporary_', tempid, sep = ""), showWarnings = FALSE)

  setwd(paste(getwd(), '/Temporary_', tempid, sep = ""))
  dir.create("./data/")
  dir.create("./data/upload/")


  tempFolder <- paste("temp_", Soft1R::create_unique_ids(1), sep = "")


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

  dir.create(paste("./data/upload/", tempFolder, sep = ""))

  readr::write_csv2(data_types, path = paste("./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies.csv", sep = ""), na = "")

  # Items segmentation
  items_data_types <-
    structure(list(itemid = "string",
                   totalsales = "float",
                   averagesales = "float",
                   totalprofit = "float",
                   averageprofit = "float",
                   transactions = "float",
                   quantitypertrans = "float",
                   meandiff = "float",
                   segment = "string",
                   clusteringnumsegments = "string",
                   pc1 = "float",
                   pc2 = "float",
                   datetimecreated = "datetime",
                   `Itemid-UniqueKey` = "string"), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, -1L))

  readr::write_csv2(items_data_types, path = paste("./data/upload/", tempFolder, "/ItemSegmentationAllCompanies.csv", sep = ""), na = "")


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

  readr::write_csv2(data_types2, path = paste("./data/upload/", tempFolder, "/CustomerSegmentationPerCompany.csv", sep = ""), na = "")


  # Items segmentation per company
  items_data_types <-
    structure(list(itemid = "string",
                   totalsales = "float",
                   averagesales = "float",
                   totalprofit = "float",
                   averageprofit = "float",
                   transactions = "float",
                   quantitypertrans = "float",
                   meandiff = "float",
                   segment = "string",
                   clusteringnumsegments = "string",
                   pc1 = "float",
                   pc2 = "float",
                   datetimecreated = "datetime",
                   cmpcode = "string",
                   `Itemid-UniqueKey` = "string"), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, -1L))

  readr::write_csv2(items_data_types, path = paste("./data/upload/", tempFolder, "/ItemSegmentationPerCompany.csv", sep = ""), na = "")


  # 1.2.2 Create empty zip files -----------------------------------------------

  dir.create(paste("./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies", sep = ""))
  dir.create(paste("./data/upload/", tempFolder, "/ItemSegmentationAllCompanies", sep = ""))


  dir.create(paste("./data/upload/", tempFolder, "/CustomerSegmentationPerCompany", sep = ""))
  dir.create(paste("./data/upload/", tempFolder, "/ItemSegmentationPerCompany", sep = ""))


  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies/CustomerSegmentationAllCompanies.zip", " ./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies.csv", sep = ""))
  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/ItemSegmentationAllCompanies/ItemSegmentationAllCompanies.zip", " ./data/upload/", tempFolder, "/ItemSegmentationAllCompanies.csv", sep = ""))


  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/CustomerSegmentationPerCompany/CustomerSegmentationPerCompany.zip", " ./data/upload/", tempFolder, "/CustomerSegmentationPerCompany.csv", sep = ""))
  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/ItemSegmentationPerCompany/ItemSegmentationPerCompany.zip", " ./data/upload/", tempFolder, "/ItemSegmentationPerCompany.csv", sep = ""))


  # 1.2.3 Upload empty zip files  ----------------------------------------------

  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies/*.zip", sep = ""), "CustomerSegmentationAllCompanies")  # uploading everything in a directory
  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/ItemSegmentationAllCompanies/*.zip", sep = ""), "ItemSegmentationAllCompanies")  # uploading everything in a directory

  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/CustomerSegmentationPerCompany/*.zip", sep = ""), "CustomerSegmentationPerCompany")  # uploading everything in a directory
  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/ItemSegmentationPerCompany/*.zip", sep = ""), "ItemSegmentationPerCompany")  # uploading everything in a directory


  # 2. Download transactional datasets -------------------------------------------

  AzureStor::multidownload_adls_file(fs, src = "/SalesTransactions/*.*", dest = paste("./data/", tempFolder, sep = ""), overwrite = TRUE)


  # 3. Unzip & Read dataset in R -----------------------------------------------
  zipfiles <- list.files(path = paste('./data/', tempFolder, sep = ""), pattern = "*.zip", full.names = TRUE) # data frame of zip files in current directory

  for (i in seq_along(zipfiles)) {

    utils::unzip(zipfiles[i], exdir = paste("./data/csv_", tempFolder, sep = ""))

  }

  # Delete empty csv files
  for (i in list.files(path = paste("./data/csv_", tempFolder, sep = ""), pattern="*.csv", full.names = TRUE)){
    if (nrow(data.table::fread(i)) <= 1) {unlink(i)}
  }


  files = list.files(path = paste("./data/csv_", tempFolder, sep = ""), pattern="*.csv", full.names = TRUE)



  vmtrstat2 <-
    data.table::rbindlist(lapply(files, data.table::fread, colClasses=list(character=1:ncol(data.table::fread(files[1]))), skip = 2)) %>%
    dplyr::as_tibble()

  names(vmtrstat2) <- names(data.table::fread(files[1]))


  vmtrstat <-
    vmtrstat2 %>%
    dplyr::mutate(
      date = with_tz(as_datetime(as.POSIXct(as_datetime(vmtrstat2$date), tz="Europe/London")), tzone = "Europe/Athens"),
      date = as.Date(as.character(date))
    ) %>%
    dplyr::filter(date > max(date) - lubridate::years(3)) %>%
    dplyr::mutate(salesval = as.numeric(gsub(",", ".", salesval)),
           salescost = as.numeric(gsub(",", ".", salescost)),
           salesqty1 = as.numeric(gsub(",", ".", salesqty1)))

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

  # Items segmentation all companies
  cluster_items_all_companies <<-
    vmtrstat %>%
    dplyr::filter(itemid != "") %>%
    dplyr::mutate(profit = salesval - salescost) %>%
    dplyr::group_by(salesid, itemid) %>%
    dplyr::summarise(
      sales = sum(salesval),
      profit = sum(profit),
      len = length(itemid)
    ) %>%
    dplyr::group_by(itemid) %>%
    dplyr::summarise(totalsales = sum(sales),
              averagesales = mean(sales),
              totalprofit = sum(profit),
              averageprofit = mean(profit),
              # median_sales_per_trans = median(sales),
              transactions = length(salesid),
              quantitypertrans = sum(len)/length(salesid)
    ) %>%
    dplyr::bind_cols(
      vmtrstat %>%
        dplyr::filter(itemid != "") %>%
        dplyr::select(date, itemid) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(itemid, date) %>%
        dplyr::group_by(itemid) %>%
        dplyr::mutate(previous = dplyr::lag(date)) %>%
        dplyr::mutate(diff = as.integer(date - previous)) %>%
        dplyr::summarise(meandiff = mean(diff, na.rm = TRUE),
                  mediandiff = stats::median(diff, na.rm = TRUE),
                  mindiff = min(diff, na.rm = TRUE),
                  maxdiff = max(diff, na.rm = TRUE),
                  quantile25 = stats::quantile(diff, probs = 0.25, na.rm = TRUE),
                  quantile75 = stats::quantile(diff, probs = 0.75, na.rm = TRUE)) %>%
        tidyr::replace_na(list(meandiff = 0)) %>%
        dplyr::select(meandiff)
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


  # Items segmentation per company
  cluster_items_per_company <<-
    vmtrstat %>%
    dplyr::filter(itemid != "") %>%
    dplyr::mutate(profit = salesval - salescost) %>%
    dplyr::group_by(cmpcode, salesid, itemid) %>%
    dplyr::summarise(
      sales = sum(salesval),
      profit = sum(profit),
      len = length(itemid)
    ) %>%
    dplyr::group_by(cmpcode, itemid) %>%
    dplyr::summarise(totalsales = sum(sales),
              averagesales = mean(sales),
              totalprofit = sum(profit),
              averageprofit = mean(profit),
              # median_sales_per_trans = median(sales),
              transactions = length(salesid),
              quantitypertrans = sum(len)/length(salesid)
    ) %>%
    dplyr::bind_cols(
      vmtrstat %>%
        dplyr::filter(itemid != "") %>%
        dplyr::select(cmpcode, date, itemid) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(cmpcode, itemid, date) %>%
        dplyr::group_by(cmpcode, itemid) %>%
        dplyr::mutate(previous = dplyr::lag(date)) %>%
        dplyr::mutate(diff = as.integer(date - previous)) %>%
        dplyr::summarise(meandiff = mean(diff, na.rm = TRUE),
                  mediandiff = stats::median(diff, na.rm = TRUE),
                  mindiff = min(diff, na.rm = TRUE),
                  maxdiff = max(diff, na.rm = TRUE),
                  quantile25 = stats::quantile(diff, probs = 0.25, na.rm = TRUE),
                  quantile75 = stats::quantile(diff, probs = 0.75, na.rm = TRUE)) %>%
        tidyr::replace_na(list(meandiff = 0)) %>%
        as.data.frame() %>%
        dplyr::select(meandiff)
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

  # Items segmentation
  item_unique <-
    vmtrstat %>%
    dplyr::group_by(itemid, cmpcode) %>%
    dplyr::do(utils::head(., 1)) %>%
    dplyr::mutate(`Itemid-UniqueKey` = paste(cmpcode, itemid, sep = "-")) %>%
    dplyr::ungroup() %>%
    dplyr::select(itemid, `Itemid-UniqueKey`)



  new_items <-
    Soft1R::cluster_s1(final_dataset = cluster_items_all_companies) %>%
    dplyr::mutate(datetimecreated = paste(as.character(lubridate::format_ISO8601(Sys.time())), ".000Z", sep = "")) %>%
    dplyr::mutate_if(is.numeric, decimal_sep) %>%
    dplyr::mutate_if(is.factor, as.character) %>%
    dplyr::left_join(item_unique)


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

  # Items Segmentation

  l <- list()

  for (i in 1:length(companies)) {

    test <-
      Soft1R::cluster_s1(final_dataset = cluster_items_per_company %>% dplyr::filter(cmpcode == companies[i]) %>% as.data.frame() %>% dplyr::select(-cmpcode)) %>%
      dplyr::mutate(datetimecreated = paste(as.character(lubridate::format_ISO8601(Sys.time())), ".000Z", sep = "")) %>%
      dplyr::mutate(cmpcode = as.character(companies[i]),
             `Itemid-UniqueKey` = paste(cmpcode, itemid, sep = "-"))


    l[[companies[i]]] <- test
  }

  new_items_per_company <-
    as.data.frame(do.call(rbind, l)) %>%
    dplyr::mutate_if(is.numeric, Soft1R::decimal_sep) %>%
    dplyr::mutate_if(is.factor, as.character)


  # 6. Update detail data objects ----------------------------------------------

   utils::write.table(new, file = paste("./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies.csv", sep = ""), sep = ";", na = "", quote=c(1, 7, 11), row.names = FALSE, col.names = FALSE, append = TRUE)
   utils::write.table(new_items, file = paste("./data/upload/", tempFolder, "/ItemSegmentationAllCompanies.csv", sep = ""), sep = ";", na = "", quote=c(1, 10, 14), row.names = FALSE, col.names = FALSE, append = TRUE)

   utils::write.table(new_per_company, file = paste("./data/upload/", tempFolder, "/CustomerSegmentationPerCompany.csv", sep = ""), sep = ";", na = "", quote=c(1, 7, 12), row.names = FALSE, col.names = FALSE, append = TRUE)
   utils::write.table(new_items_per_company, file = paste("./data/upload/", tempFolder, "/ItemSegmentationPerCompany.csv", sep = ""), sep = ";", na = "", quote=c(1, 10, 14, 15), row.names = FALSE, col.names = FALSE, append = TRUE)


   # 7. Update the zip files ----------------------------------------------------

  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies/CustomerSegmentationAllCompanies.zip", " ./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies.csv", sep = ""))
  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/ItemSegmentationAllCompanies/ItemSegmentationAllCompanies.zip", " ./data/upload/", tempFolder, "/ItemSegmentationAllCompanies.csv", sep = ""))

  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/CustomerSegmentationPerCompany/CustomerSegmentationPerCompany.zip", " ./data/upload/", tempFolder, "/CustomerSegmentationPerCompany.csv", sep = ""))
  system(paste("zip -9 -y -j -q ", "./data/upload/", tempFolder, "/ItemSegmentationPerCompany/ItemSegmentationPerCompany.zip", " ./data/upload/", tempFolder, "/ItemSegmentationPerCompany.csv", sep = ""))


  # 8. Upload updated zip files ------------------------------------------------

  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/CustomerSegmentationAllCompanies/*.zip", sep = ""), "CustomerSegmentationAllCompanies")  # uploading everything in a directory
  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/ItemSegmentationAllCompanies/*.zip", sep = ""), "ItemSegmentationAllCompanies")  # uploading everything in a directory

  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/CustomerSegmentationPerCompany/*.zip", sep = ""), "CustomerSegmentationPerCompany")  # uploading everything in a directory
  AzureStor::storage_multiupload(cont, paste("./data/upload/", tempFolder, "/ItemSegmentationPerCompany/*.zip", sep = ""), "ItemSegmentationPerCompany")  # uploading everything in a directory

  # 9. Create log files --------------------------------------------------------

  time_n <- proc.time() - ptm

  time_now2 <- as.character(format(Sys.time(), "%Y_%m_%d_%H_%M_%S"))

  segmentation <-
  new %>%
    group_by(clusteringnumsegments, segment) %>%
    summarise(N = n()) %>%
    mutate(type = "customer") %>%
    bind_rows(
    new_items %>%
    group_by(clusteringnumsegments, segment) %>%
    summarise(N = n()) %>%
    mutate(type = "item")
    ) %>%
    mutate(code = code,
           date = time_now2,
           vmtrstat_rows = nrow(vmtrstat),
           customers = length(unique(vmtrstat$custid)),
           products = length(unique(vmtrstat$itemid)),
           duration =   round(as.numeric(time_n[3]), 1))


  dir.create("../Logs", showWarnings = FALSE)

  write.csv2(segmentation, file = paste0("../Logs/", time_now2, "_Segmentation.csv", sep = ""), row.names = FALSE)


  # 10.  Clean disk ------------------------------------------------------------
  if (str_sub(getwd(), -20, -11) == "Temporary_") {setwd("../")}
  unlink(paste(getwd(), '/Temporary_', tempid, sep = ""), recursive = T)

}
