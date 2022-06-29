


#' Generate unique ids
#'
#' @description
#' Generate unique ids. It can be used for generating unique keys of different lengths
#'
#' @param n Specify number of ids to generate
#' @param char_len Specify the character length of the ID (default is 10)
#'
#' @details
#'
#' - Can be used to generate unique paths.
#'
#' @examples
#'
#' create_unique_ids(5, 50)
#'
#' @export

create_unique_ids <- function(n, char_len = 10){

  pool <- c(letters, LETTERS, 0:9)

  res <- character(n) # pre-allocating vector is much faster than growing it
  for(i in seq(n)){
    this_res <- paste0(sample(pool, char_len, replace = TRUE), collapse = "")
    while(this_res %in% res){ # if there was a duplicate, redo
      this_res <- paste0(sample(pool, char_len, replace = TRUE), collapse = "")
    }
    res[i] <- this_res
  }
  res
}
