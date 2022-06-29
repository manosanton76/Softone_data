
# Forecasting sales & Receipts -------------------------------------------------

# At first i must create timing objects in main fuctions
library(tictoc)

tic()
Sys.sleep(1)
new_time <- toc()
new_toc <- new_time$toc - new_time$tic


tic()
Sys.sleep(1)
new_per_company_time <- toc()
new_per_company_toc <- new_per_company_time$toc - new_per_company_time$tic



# I can create a new function to run the below & store it somewhere????
new %>%
  group_by(timeperiod, anomaly, finalmodeltype, finalmodelmape, finalmodelrmse) %>%
  filter(type == "actual") %>%
  summarise(start = min(as.Date(date)),
            end = max(as.Date(date)),
            mean_metric = mean(as.numeric(metric)),
            median_metric = median(as.numeric(metric)),
            prop_01_metric = quantile(as.numeric(metric), probs = c(.01)),
            prop_05_metric = quantile(as.numeric(metric), probs = c(.05)),
            prop_10_metric = quantile(as.numeric(metric), probs = c(.10)),
            prop_25_metric = quantile(as.numeric(metric), probs = c(.25)),
            prop_75_metric = quantile(as.numeric(metric), probs = c(.75)),
            prop_90_metric = quantile(as.numeric(metric), probs = c(.90)),
            prop_95_metric = quantile(as.numeric(metric), probs = c(.95)),
            prop_99_metric = quantile(as.numeric(metric), probs = c(.99)),
            sd_metric = sd(as.numeric(metric)),
            min_metric = min(as.numeric(metric)),
            max_metric = max(as.numeric(metric))
            ) %>%
  add_column(cmpcode = "all",
             time_elapsed = new_toc) %>%
  select(cmpcode, everything()) %>%
  bind_rows(
    new_per_company %>%
    group_by(cmpcode, timeperiod, anomaly, finalmodeltype, finalmodelmape, finalmodelrmse) %>%
    filter(type == "actual") %>%
    summarise(start = min(as.Date(date)),
            end = max(as.Date(date)),
            mean_metric = mean(as.numeric(metric)),
            median_metric = median(as.numeric(metric)),
            prop_01_metric = quantile(as.numeric(metric), probs = c(.01)),
            prop_05_metric = quantile(as.numeric(metric), probs = c(.05)),
            prop_10_metric = quantile(as.numeric(metric), probs = c(.10)),
            prop_25_metric = quantile(as.numeric(metric), probs = c(.25)),
            prop_75_metric = quantile(as.numeric(metric), probs = c(.75)),
            prop_90_metric = quantile(as.numeric(metric), probs = c(.90)),
            prop_95_metric = quantile(as.numeric(metric), probs = c(.95)),
            prop_99_metric = quantile(as.numeric(metric), probs = c(.99)),
            sd_metric = sd(as.numeric(metric)),
            min_metric = min(as.numeric(metric)),
            max_metric = max(as.numeric(metric))) %>%
    add_column(time_elapsed = new_per_company_toc)
    ) %>%
  add_column(code = code,
             rows = nrow(vmtrstat2)) %>%
  View()


# Segmentation

library(tictoc)

tic()
Sys.sleep(1)
new_time <- toc()
new_toc <- new_time$toc - new_time$tic

tic()
Sys.sleep(1)
new_items_time <- toc()
new_items_toc <- new_items_time$toc - new_items_time$tic


new %>%
  # filter(clusteringnumsegments == "4") %>%
  group_by(clusteringnumsegments, segment, datetimecreated) %>%
  summarise(customers = n(),
            median_sales_median = median(as.numeric(mediansales), na.rm = TRUE),
            total_sales_median = mean(as.numeric(totalsales), na.rm = TRUE),
            recency_median = median(as.numeric(recency), na.rm = TRUE),
            recency_mean = mean(as.numeric(recency), na.rm = TRUE),
            transactions_mean = mean(as.numeric(transactions), na.rm = TRUE),
            transactions_median = median(as.numeric(transactions), na.rm = TRUE)
            ) %>%
  mutate(total_customers = length(unique(new$`Custid-UniqueKey`)),
         time_elapsed = new_toc,
         code = code) %>%
  select(code, datetimecreated, time_elapsed, everything()) %>% View()


new_items %>%
  group_by(clusteringnumsegments, segment, datetimecreated) %>%
  summarise(items = n(),
            diff_mean = mean(as.numeric(meandiff), na.rm = TRUE),
            diff_median = median(as.numeric(meandiff), na.rm = TRUE),
            transactions_mean = mean(as.numeric(transactions), na.rm = TRUE),
            transactions_median = median(as.numeric(transactions), na.rm = TRUE),
            total_sales_median = median(as.numeric(totalsales), na.rm = TRUE),
            total_sales_mean = mean(as.numeric(totalsales), na.rm = TRUE),
            total_profit_median = median(as.numeric(totalprofit), na.rm = TRUE),
            total_profit_mean = mean(as.numeric(totalprofit), na.rm = TRUE),
            total_profit_sum = sum(as.numeric(totalprofit), na.rm = TRUE)

  ) %>%
  mutate(total_items = length(unique(new_items$`Itemid-UniqueKey`)),
         time_elapsed = new_items_toc,
         code = code) %>%
  select(code, datetimecreated, time_elapsed, everything()) %>% View()

