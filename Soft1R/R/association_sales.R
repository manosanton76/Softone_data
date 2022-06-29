#' Perform Market Basket analysis on sales transactions
#'
#' @description
#' It produces the appropriate data structures for the market basket analysis
#' on the sales dataset of the installation (joined with items information)
#'
#' @param code Soft1 installation code
#' @param datalake Which Azure data lake to use (0 for production)
#'
#' @details
#'
#' - Must provide a valid Soft1 installation code i.e. must have data in azure data lake
#' - Use 0 on datalake, for storing results on the production Azure data lake.
#' Use 1 (default) for the dev Azure data lake
#'
#'
#' @examples
#' \dontrun{
#'
#' library(tidyverse)
#' library(lubridate)
#' library(Soft1R)
#' library(tidyquant)
#' library(AzureAuth)
#' library(AzureStor)
#' library(data.table)
#' library(parsedate)
#' library(recommenderlab)
#' library(arules)
#' library(arulesViz)
#'
#' association(code = "01100201021801", datalake = 1)
#'
#' }
#'
#' @export
#'

association <- function(code, datalake = 1) {

ptm <- proc.time()


# 1. Get Access to Azure data lake ---------------------------------------------
if (datalake == 0 ) {
  token_path <- "https://s1datalakeprod01.blob.core.windows.net"
  main_path <- "https://s1datalakeprod01.dfs.core.windows.net"
} else {
  token_path <- "https://s1azdatalake01.blob.core.windows.net"
  main_path <- "https://s1azdatalake01.dfs.core.windows.net"
}


token <- AzureAuth::get_azure_token(, auth_type="client_credentials")

ad_endp_tok <- AzureStor::storage_endpoint(main_path, token=token)

cont <- AzureStor::storage_container(ad_endp_tok, code)

files <- AzureStor::list_storage_files(cont)

fs <<- AzureStor::adls_filesystem(paste(main_path, "/", code, sep = ""), token=token)

# 1.1 Set up working directory & folders -------------------------------------

temp_path <<- "/association"
dir.create(paste(tempdir(), "/association/", sep = ""))

dir.create(paste(tempdir(), temp_path, "/upload/", sep = ""))
dir.create(paste(tempdir(), temp_path, "/data/", sep = ""))

# 1.2 Upload Empty files -----------------------------------------------------

# 1.2.1 Create empty detail data objects -------------------------------------
data_types <-
  structure(list(rules = "string",
                 support = "float",
                 confidence = "float",
                 lift = "float",
                 count = "float",
                 datetimecreated = "datetime"), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, -1L))

readr::write_csv2(data_types, path = paste(tempdir(), temp_path, "/upload", "/AssociationsAllCompanies.csv", sep = ""), na = "", )


# 1.2.2 Create empty zip files -----------------------------------------------

dir.create(paste(tempdir(), temp_path, "/upload/AssociationsAllCompanies", sep = ""))

system(paste("zip -9 -y -j -q ", tempdir(), temp_path, "/upload/AssociationsAllCompanies/AssociationsAllCompanies.zip", " ", tempdir(), temp_path, "/upload/AssociationsAllCompanies.csv", sep = ""))


# 1.2.3 Upload empty zip files  ----------------------------------------------

AzureStor::storage_multiupload(cont, paste(tempdir(), temp_path, "/upload/", "/AssociationsAllCompanies/*.zip", sep = ""), "AssociationsAllCompanies")  # uploading everything in a directory

# 2.1 Download Sales Transactions ----------------------------------------------

AzureStor::multidownload_adls_file(fs, src = "/SalesTransactions/*.*", dest = paste(tempdir(), temp_path, "/data/", sep = ""), overwrite = TRUE)

# 3. Read datasets in R --------------------------------------------------------

zipfiles <- list.files(path = paste(tempdir(), temp_path, "/data/", sep = ""), pattern = "*.zip", full.names = TRUE) # data frame of zip files in current directory


for (i in seq_along(zipfiles)) {

  utils::unzip(zipfiles[i], exdir = paste(tempdir(), temp_path, "/data/csv", sep = ""))

}

# Delete empty csv files
for (i in list.files(path = paste(tempdir(), temp_path, "/data/csv", sep = ""), pattern="*.csv", full.names = TRUE)){
  if (nrow(data.table::fread(i)) <= 1) {unlink(i)}
}



files = list.files(path = paste(tempdir(), temp_path, "/data/csv", sep = ""), pattern="*.csv", full.names = TRUE)


vmtrstat <<-
  data.table::rbindlist(lapply(files, data.table::fread, colClasses=list(character=1:ncol(data.table::fread(files[1]))), skip = 2)) %>%
  dplyr::as_tibble() %>%
  rename_with(~ names(data.table::fread(files[1])))


vmtrstat <<-
  vmtrstat %>%
  dplyr::mutate(
    salesval = as.numeric(gsub(",", ".", salesval)),
    # date = as.Date(date),
    date = with_tz(as_datetime(as.POSIXct(as_datetime(vmtrstat$date), tz="Europe/London")), tzone = "Europe/Athens"),
    date = as.Date(as.character(date))
  )




## 3.1 Download & insert rest datasets in R ------------------------------------


Items <- Soft1R::download_folder("/Items/")

ItemAccCateg <- Soft1R::download_folder("/ItemAccCateg/")

ItemBrand <- Soft1R::download_folder("/ItemBrand/")

ItemComCateg <- Soft1R::download_folder("/ItemComCateg/")

ItemGroup <- Soft1R::download_folder("/ItemGroup/")

ItemManfctr <- Soft1R::download_folder("/ItemManfctr/")

ItemModel <- Soft1R::download_folder("/ItemModel/")

ItemSeason <- Soft1R::download_folder("/ItemSeason/")




# 3.2 Merge datasets -----------------------------------------------------------


items_all <-
  Items %>%
  dplyr::left_join(ItemAccCateg, by = c("itemcmp" = "itemacccategorycmp", "itemacccategory" = "itemacccategory")) %>%
  dplyr::left_join(ItemBrand, by = c("itemcmp" = "itembrandcmp", "itembrand" = "itembrand")) %>%
  dplyr::left_join(ItemComCateg, by = c("itemcmp" = "itemcomcatcmp", "itemcomcat" = "itemcomcat")) %>%
  dplyr::left_join(ItemGroup, by = c("itemcmp" = "itemgroupcmp", "itemgroup" = "itemgroup")) %>%
  dplyr::left_join(ItemManfctr, by = c("itemcmp" = "itemmanfctrcmp", "itemmanfctr" = "itemmanfctr")) %>%
  dplyr::left_join(ItemModel, by = c("itemcmp" = "itemmodelcmp", "itemmodel" = "itemmodel")) %>%
  dplyr::left_join(ItemSeason, by = c("itemcmp" = "itemseasoncmp", "itemseason" = "itemseason"))



if (is.null(vmtrstat$invkind[1]) == TRUE) {
  sales <-
    vmtrstat %>%
    dplyr::left_join(items_all, by = c("cmpcode" = "itemcmp", "itemid" = "itemid")) %>%
    dplyr::filter(date >= max(date) - lubridate::years(2))
} else {
  sales <-
    vmtrstat %>%
    dplyr::left_join(items_all, by = c("cmpcode" = "itemcmp", "itemid" = "itemid")) %>%
    dplyr::filter(date >= max(date) - lubridate::years(2)) %>%
    dplyr::filter(invkind == "retail")

}



# 4. Create datasets for Market Basket analysis --------------------------------
# item_frequency_tbl

item_frequency_tbl <- sales %>%
  dplyr::count(itemname, itemid) %>%
  dplyr::arrange(desc(n)) %>%
  dplyr::mutate(
    pct = n / sum(n),
    cumulative_pct = cumsum(pct),
    popular_product = ifelse(cumulative_pct <= 0.5, "Yes", "No")
  ) %>%
  tibble::rowid_to_column(var = "rank") %>%
  dplyr::mutate(label_text = str_glue("Rank: {rank}
                                 Product: {itemname}
                                 ProductID: {itemid}
                                 Count: {n}
                                 Pct: {scales::percent(pct)}
                                 Cumulative Pct: {scales::percent(cumulative_pct)}"))



# user_frequency_tbl

user_frequency_tbl <- sales %>%
  dplyr::distinct(custid, salesid) %>%
  dplyr::count(custid) %>%
  dplyr::arrange(desc(n)) %>%
  dplyr::mutate(
    pct = n / sum(n),
    cumulative_pct = cumsum(pct),
    popular_customer = ifelse(cumulative_pct <= 0.5, "Yes", "No")
  ) %>%
  tibble::rowid_to_column(var = "rank")



# user_item_frequency_tbl

user_item_frequency_tbl <- sales %>%
  dplyr::count(custid) %>%
  dplyr::arrange(desc(n)) %>%
  dplyr::mutate(
    pct = n / sum(n),
    cumulative_pct = cumsum(pct),
    popular_customer = ifelse(cumulative_pct <= 0.5, "Yes", "No")
  ) %>%
  tibble::rowid_to_column(var = "rank")



# Useritem-item/ Transaction-item matrix

#  Popular Products
top_products_vec <- item_frequency_tbl %>%
  dplyr::filter(rank < 2500) %>%
  dplyr::pull(itemname)

# Use names to filter
top_products_basket_tbl <- sales %>%
  dplyr::filter(itemname %in% top_products_vec)

# Large Baskets
top_users_vec <- user_item_frequency_tbl %>%
  dplyr::filter(rank < 2500 & n < 150) %>%
  dplyr::pull(custid)

market_basket_condensed_tbl <- top_products_basket_tbl %>%
  dplyr::filter(custid %in% top_users_vec)


# FORMAT DATA

#  "Binary Ratings Matrix"
# - Did basket contain an item (Yes/No encoded as 1-0)
# - See also "Real Ratings Matrix" for "Amazon"-style ratings 1-5
market_basket_condensed_tbl <-
  market_basket_condensed_tbl %>%
  dplyr::group_by(salesid, itemname) %>%
  dplyr::summarise(N = n()) %>%
  dplyr::ungroup()


user_item_tbl <- market_basket_condensed_tbl %>%
  dplyr::select(salesid, itemname) %>%
  dplyr::mutate(value = 1) %>%
  tidyr::spread(itemname, value, fill = 0)

user_item_rlab <- user_item_tbl %>%
  dplyr::select(-salesid) %>%
  as.matrix() %>%
  as("binaryRatingMatrix")


# 5. Build models & create association rules dataset ----------------------------

# Run association rules
model_ar <- recommenderlab::Recommender(
  data = user_item_rlab,
  method = "AR",
  param = list(supp = 0.001, conf = 0.001))


rules <- model_ar@model$rule_base

associations <-
  as(rules, "data.frame") %>%
  dplyr::arrange(-lift) %>%
  dplyr::select(-coverage) %>%
  dplyr::mutate(datetimecreated = paste(as.character(lubridate::format_ISO8601(Sys.time())), ".000Z", sep = "")) %>%
  dplyr::mutate_if(is.numeric, decimal_sep)



# 6. Update detail data objects ----------------------------------------------

utils::write.table(associations, file = paste(tempdir(), temp_path, "/upload", "/AssociationsAllCompanies.csv", sep = ""), sep = ";", na = "", quote=c(1), row.names = FALSE, col.names = FALSE, append = TRUE)

# 7. Update the zip files ----------------------------------------------------

system(paste("zip -9 -y -j -q ", tempdir(), temp_path, "/upload/AssociationsAllCompanies/AssociationsAllCompanies.zip", " ", tempdir(), temp_path, "/upload/AssociationsAllCompanies.csv", sep = ""))

# 8. Upload updated zip files  -----------------------------------

AzureStor::storage_multiupload(cont, paste(tempdir(), temp_path, "/upload/", "/AssociationsAllCompanies/*.zip", sep = ""), "AssociationsAllCompanies")  # uploading everything in a directory

# 9. Create log files --------------------------------------------------------

time_n <- proc.time() - ptm

time_now2 <- as.character(format(Sys.time(), "%Y_%m_%d_%H_%M_%S"))


associations_stats <-
  associations %>%
  dplyr::summarise(lift_max = max(lift),
            lift_over1 = length(lift[lift > 1])) %>%
  dplyr::mutate(code = code,
         date = time_now2,
         vmtrstat_rows = nrow(vmtrstat),
         customers = length(unique(vmtrstat$custid)),
         products = length(unique(vmtrstat$itemid)),
         duration =   round(as.numeric(time_n[3]), 1))

dir.create("./Logs", showWarnings = FALSE)

write.csv2(associations_stats, file = paste0("./Logs/", time_now2, "_Association_Stats.csv", sep = ""), row.names = FALSE)

# Define the blob storage
cont <- AzureStor::blob_container(
  "https://softonebilogs.blob.core.windows.net/softonebilogs",
  sas="sp=racwli&st=2022-04-01T13:42:50Z&se=2022-12-31T22:42:50Z&spr=https&sv=2020-08-04&sr=c&sig=mIpQrdjrjRmaOeWxR8j24k%2BVtH1JjajPk5CtOi6s9Cs%3D")


AzureStor::upload_blob(cont, paste0("./Logs/", time_now2, "_Association_Stats.csv", sep = ""))

# 10. Clean disk -------------------------------------------------------------
unlink(paste(tempdir(), temp_path, sep = ""), recursive = T)

}
