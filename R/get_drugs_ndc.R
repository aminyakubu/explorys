#' Get a dataset with flags indicating if patient has a medication or not using NDC codes
#'
#' This function provides a dataframe with flags indicating if a patient has a medication or not. The flags are 1 which
#' indicate patient had the medication or 0 which indicates patient did not. A missing flag means that the patient did not
#' get any medication at all in the time frame specified. All arguments are strings
#'
#' @param supermart Name of the supermart where the data is stored. Eg. supermart_392
#' @param sandbox Name of the writable location usually sandbox where cohort is saved
#' @param cohort Name of the cohort with explorys_patient_id's.
#' @drug_list_df Name of the dataframe containing the NDC codes. This dataframe must be available in local environment as well as
#' on the sandbox
#' @ndc_col Name of the column in the drug_list_df specified. This column has the NDC codes
#' @grouping_col Name of the column that has the groups of medications of interest
#' @index_col Name of the column with the index dates
#' @date_condition The sql format date condition. For example "DATE(diagnosis_date) BETWEEN '2019-01-01' AND '2020-01-01'"
get_drug_ndc <- function(supermart, sandbox, cohort, drug_list_df, ndc_col, grouping_col, index_col, date_condition){

    # First let's remove the hyphens from the data
    sql1 <- glue::glue("
                      CREATE TABLE {sandbox}.temp_meds AS
                       SELECT d.explorys_patient_id, d.prescription_date, {index_col},
                        REPLACE(d.ndc_code, '-', '') AS ndc_code,
                        REPLACE(d.all_ndc_codes, '-', '') AS all_ndc_codes,
                        REPLACE(d.display_ndc, '-', '') AS display_ndc
                       FROM {supermart}.v_drug AS d
                       INNER JOIN {sandbox}.{cohort} AS c
                       ON d.explorys_patient_id = c.explorys_patient_id",
                       sandbox = sandbox,
                       cohort = cohort,
                       index_col = index_col,
                       supermart = supermart)

    drop(sandbox, table = "temp_meds")
    dbSendUpdate(sm, sql1)

    # Now let's convert from long to wide format

    sql2 <- glue::glue("
                      CREATE TABLE {sandbox}.temp_drugs_ndc AS
                            select explorys_patient_id, prescription_date, {index_col}, words AS ndc
                       FROM (select explorys_patient_id, prescription_date, {index_col},
                       StringTokenizerDelim(all_ndc_codes, ',')
                       OVER(PARTITION BY explorys_patient_id, prescription_date, {index_col} ORDER BY explorys_patient_id)
                       FROM {sandbox}.temp_meds) AS derived
                       UNION ALL

                       SELECT explorys_patient_id, prescription_date, {index_col}, ndc_code AS ndc
                       FROM {sandbox}.temp_meds

                       UNION ALL

                       SELECT explorys_patient_id, prescription_date, {index_col}, display_ndc AS ndc
                       FROM {sandbox}.temp_meds",
                       sandbox = sandbox,
                       cohort = cohort,
                       index_col = index_col)

    drop(sandbox, table = "temp_drugs_ndc")
    dbSendUpdate(sm, sql2)

    select_max <- function(){
        groups = get(drug_list_df)[grouping_col]
        vec = as.list(unique(groups))

        statement <- paste0('MAX(', unlist(vec, use.names = F), ') AS ', unlist(vec, use.names = F), collapse = ', ')
        return(statement)}

    make_case <- function(){
        groups = get(drug_list_df)[grouping_col]
        vec = pull(unique(groups))
        statement <- sapply(vec, function(vec) {
            #deparse(substitute(drug_ndc_list))
            paste0('CASE WHEN ndc IN (',
                   select_sql_str(drug_list_df, ndc_col, vec),
                   ") THEN '1' ELSE '0' END AS ",
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
                      FROM {sandbox}.temp_drugs_ndc

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






