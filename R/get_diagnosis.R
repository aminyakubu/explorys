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
#' @dx_codes_col Name of the column in the dx_codes_df specified. This column has the icd9/10/snomed codes
#' @grouping_col Name of the column that has the groups of medications of interest
#' @index_col Name of the column with the index dates
#' @date_condition The sql format date condition. For example "DATE(diagnosis_date) BETWEEN '2019-01-01' AND '2020-01-01'"
#'
get_diagnosis <- function(supermart, sandbox, cohort, dx_codes_df, dx_codes_col, grouping_col, index_col, date_condition){

    # First let's get snomed and icd codes in one column so it's easy to search
    sql1 <- glue::glue("
                       CREATE TABLE {sandbox}.temp_diags AS
                       SELECT d.explorys_patient_id, DATE(d.diagnosis_date) AS diagnosis_date, REPLACE(icd_code, '.', '') AS diag_code,
                                {index_col}, 'icd' AS code_type
                       FROM {supermart}.v_diagnosis AS d
                       INNER JOIN {sandbox}.{cohort} AS c
                       ON d.explorys_patient_id = c.explorys_patient_id
                       WHERE d.icd_code IS NOT NULL

                       UNION

                       SELECT vd.explorys_patient_id, DATE(vd.diagnosis_date) AS diagnosis_date,
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
                       SELECT explorys_patient_id,
                       {select_max_statement}
                       FROM
                       (SELECT DISTINCT explorys_patient_id,
                       {case}
                       FROM {sandbox}.temp_diags

                       WHERE {date_condition} ) as A
                       GROUP BY explorys_patient_id",
                       case = make_case(),
                       select_max_statement = select_max(),
                       date_condition = date_condition)

    results = dbGetQuery(sm, sql3)

    sql4 <- glue::glue("SELECT explorys_patient_id FROM {sandbox}.{cohort}",
                       sandbox = sandbox,
                       cohort = cohort)
    pts = dbGetQuery(sm, sql4)

    output = left_join(pts, results, by = 'explorys_patient_id')
    return(output)

}


