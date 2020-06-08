#' Easily drop tables from explorys database
#'
#' Takes in the name of the sandbox and table as character strings
#'
#' @param sandbox Name of the writable location usually sandbox where cohort is saved.
#' @param table Name of the table to be deleted.
#' @return Nothing is returned but the data is deleted from the sandbox
#' @export

drop <- function(sandbox, table){
  sql <- glue::glue(
    "DROP TABLE IF EXISTS {sandbox}.{tbl}",
    tbl = table,
    sandbox = sandbox
  )
  
  return(dbSendUpdate(sm, sql))
}