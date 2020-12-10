#' Get a dataset with flags indicating if patient has a diagnosis
#'
#' This function provides a dataframe with flags indicating if a patient has a diagnosis or not based on icd9, icd10 and snomed codes.
#' The flags are 1 whichindicate patient had the medication or 0 which indicates patient did not. A missing flag means that
#' the patient did not get any diagnosis at all in the time frame specified. All arguments are strings
#'
#' @param supermart Name of the supermart where the data is stored. Eg. supermart_392
#' @param sandbox Name of the writable location usually sandbox where cohort is saved
#' @param cohort Name of the cohort with explorys_patient_id's.
#' @dx_codes_df Name of the dataframe containing the icd 9/10/snomed codes. This dataframe must be available in local environment as well as
#' on the sandbox
#' @param dx_codes_col Name of the column in the dx_codes_df specified. This column has the icd9/10/snomed codes
#' @param grouping_col Name of the column that has the groups of medications of interest
#' @param index_col Name of the column with the index dates
#' @param extra_grouping_vars Character string of additional variables separated by comma to grouping variables
#' @param date_condition The sql format date condition. For example "DATE(diagnosis_date) BETWEEN '2019-01-01' AND '2020-01-01'"
#' @return A dataframe with indicators of diagnoses
#' @export
#'
get_diagnosis <- function(supermart, sandbox, cohort, dx_codes_df, dx_codes_col, grouping_col, index_col,
                          date_condition, extra_grouping_vars = ''){

    if (extra_grouping_vars == '') {
        group_var = ''

    } else if (!is.na(extra_grouping_vars)) {
        group_var = paste0(", ", extra_grouping_vars)

    }

    # First let's get snomed and icd codes in one column so it's easy to search
    sql1 <- glue::glue("
                       CREATE TABLE {sandbox}.temp_diags AS
                       SELECT d.explorys_patient_id {group_var}, DATE(d.diagnosis_date) AS diagnosis_date, REPLACE(icd_code, '.', '') AS diag_code,
                                {index_col}, 'icd' AS code_type
                       FROM {supermart}.v_diagnosis AS d
                       INNER JOIN {sandbox}.{cohort} AS c
                       ON d.explorys_patient_id = c.explorys_patient_id
                       WHERE d.icd_code IS NOT NULL

                       UNION

                       SELECT vd.explorys_patient_id {group_var}, DATE(vd.diagnosis_date) AS diagnosis_date,
                                vtsi.snomed_id AS diag_code, {index_col}, 'snomed' AS code_type
                       FROM {supermart}.v_diagnosis vd
                       INNER JOIN {supermart}.v_tokenized_snomed_ids vtsi
                       ON vtsi.snomed_join_id = vd.snomed_join_id
                       INNER JOIN {sandbox}.{cohort} AS c
                       ON vd.explorys_patient_id = c.explorys_patient_id",
                       sandbox = sandbox,
                       cohort = cohort,
                       index_col = index_col,
                       supermart = supermart)

    drop(sandbox, table = "temp_diags")
    dbSendUpdate(sm, sql1)


    select_max <- function(){
        groups = get(dx_codes_df)[grouping_col]
        vec = as.list(unique(groups))

        statement <- paste0('MAX(', unlist(vec, use.names = F), ') AS ', unlist(vec, use.names = F), collapse = ', ')
        return(statement)}

    select_sql_str <- function(table, var, str) {

        sql <- glue::glue(
            "SELECT DISTINCT {var} FROM {sandbox_name}.{tbl} WHERE {grouping_col} = '{string}' ",
            string = str,
            tbl = table,
            var = var,
            sandbox_name = sandbox
            #grouping_col_name = grouping_col
        )

        list <- dbGetQuery(sm, sql)

        paste0("'",
               unlist(list, use.names = FALSE),
               "'", collapse = ",")
    }

    make_case <- function(){
        groups = get(dx_codes_df)[grouping_col]
        vec = pull(unique(groups))
        statement <- sapply(vec, function(vec) {
            paste0('CASE WHEN diag_code IN (',
                   select_sql_str(dx_codes_df, dx_codes_col, vec),
                   ") THEN 1 ELSE 0 END AS ",
                   vec, collapse = ', ')}
        )
        return(paste0(statement, collapse = ' ,'))
    }

    sql3 <- glue::glue("
                       SELECT explorys_patient_id {group_var},
                       {select_max_statement}
                       FROM
                            (SELECT DISTINCT explorys_patient_id {group_var},
                               {case}
                             FROM {sandbox}.temp_diags
                             WHERE {date_condition}) AS a
                       GROUP BY explorys_patient_id {group_var}
                       ",
                       case = make_case(),
                       select_max_statement = select_max(),
                       date_condition = date_condition)

    results = dbGetQuery(sm, sql3)
    return(results)

}

# Testing
# library(RJDBC)
# library(magrittr)
# library(tidyselect)
# library(tidyr)
# library(dplyr)
# library(stringr)
# library(data.table)
#
# supermart = 'supermart_472'
# sandbox = 'SANDBOX_GENESIS_BMS_COVID'
# cohort = 'cohort_pts'
# dx_codes_df = 'dx_codes'
# dx_codes_col = 'code_clean'
# grouping_col = 'grp'
# index_col = 'index_dt'
# date_condition = '(DATE(diagnosis_date) BETWEEN CAST(index_dt AS DATE) - 180 AND CAST(index_dt AS DATE))'
# extra_grouping_vars = 'pt_group'
#
# dx_codes = readxl::read_excel('Y:/genesis-research_global/BMS_COVID/data/Charlson_Comorbidity.xlsx',
#                               sheet = 'Sheet2',
#                               col_names = TRUE) %>%
#     mutate(x = str_replace(Code, '\\.','')) %>%
#     mutate(code_clean = str_replace(x, '%', ''))
#
#
# drop("SANDBOX_GENESIS_BMS_COVID", "dx_codes")
# dbWriteTable(sm, "SANDBOX_GENESIS_BMS_COVID.dx_codes", dx_codes)






