

#' Replace . with ,
#'
#' @description
#' Replace . with , It's mainly used for replacing numerical values on other regional settings
#'
#' @param input Specify the input
#'
#' @details
#'
#' - Mainly used for transforming decimal numbers for Greek systems.
#'
#' @examples
#'
#' decimal_sep("18.8")
#'
#' @export

decimal_sep <- function(input){
  as.character(gsub("\\.", ",", input))

}
