


#' Apply clustering in a dataset
#'
#' @description
#' returns a dataset combining 5 separate datasets with clustering results
#' (for k = 2,3,4,5,6), after providing a suitable dataset
#'
#' @param final_dataset Specify the dataframe
#'
#' @details
#'
#' - Must provide a dataframe with a categorical variable (first variable) and 2 or more continuous variables
#' - The provided dataframe cannot include missing values
#'
#' @examples
#'
#' library(tidyverse)
#'
#' t <- cluster_s1(final_dataset = cluster_data_test)
#' t
#'
#' @export

cluster_s1 <- function(final_dataset = cluster_data_all){

  removeZeroVar1 <- function(df){
    df[, sapply(df, var) != 0]
  }

  final_dataset2 <- final_dataset[, -1]
  final_dataset2 <- removeZeroVar1(final_dataset2)


  set.seed(15)
  pca_soft <- stats::prcomp(scale(final_dataset2))

  # Model customers
  l <- list()
  for (k in 2:6){

    set.seed(15)

    final_dataset2 <- final_dataset[, -1]
    final_dataset2 <- removeZeroVar1(final_dataset2)

    # Build a k-means model
    model_customers <- stats::kmeans(scale(final_dataset2), centers = k)

    test <-
      final_dataset %>%
      dplyr::bind_cols(segment = factor(model_customers$cluster)) %>%
      dplyr::bind_cols(clusteringnumsegments = k,
             pc1 = pca_soft[["x"]][,"PC1"],
             pc2 = pca_soft[["x"]][,"PC2"])

    l[[k]] = test

  }

  cluster_customers <-
    as.data.frame(do.call("rbind",l))

}
