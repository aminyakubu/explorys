#' Get a dataset with flags indicating if patient has a medication or not using generic drug names
#'
#' This function returns a dataframe with flags indicating if a patient has a medication or not. The flags are 1 which
#' indicate patient had the medication or 0 which indicates patient did not. A missing flag means that the patient did not
#' get any medication at all in the time frame specified. All arguments are strings
#'
#' @param supermart Name of the supermart where the data is stored. Eg. supermart_392
#' @param sandbox Name of the writable location usually sandbox where cohort is saved
#' @param cohort Name of the cohort with explorys_patient_id's.
#' @param drug_list_df Name of the dataframe containing the medications of interest. This dataframe must be available in local environment as well as
#' on the sandbox
#' @param drug_names_col Name of the column in the drug_list_df specified. This column has the drug names of interest
#' @param grouping_col Name of the column that has the groups of medications of interest
#' @param index_col Name of the column with the index dates
#' @param date_condition The sql format date condition. For example "DATE(diagnosis_date) BETWEEN '2019-01-01' AND '2020-01-01'"
#' @param extra_grouping_vars Character string of additional variables separated by comma to grouping variables
#' @return A dataframe with indicators of medication usage
#' @export
get_drugs_string = function(supermart, sandbox, cohort, drug_list_df, grouping_col, drug_names_col, index_col,
                            date_condition, extra_grouping_vars = ''){


    if (extra_grouping_vars == '') {
        group_var = ''

    } else if (!is.na(extra_grouping_vars)) {
        group_var = paste0(", ", extra_grouping_vars)

    }

    # Let's get the appropriate dataset we will be using

    sql1 <- glue::glue("CREATE TABLE {sandbox}.temp_meds AS
                       SELECT d.*, {index_col} {group_var}
                       FROM {supermart}.v_drug AS d
                       INNER JOIN {sandbox}.{cohort} AS c
                       ON d.explorys_patient_id = c.explorys_patient_id",
                       sandbox = sandbox,
                       cohort = cohort,
                       index_col = index_col,
                       supermart = supermart)

    drop(sandbox, table ="temp_meds")
    dbSendUpdate(sm, sql1)

    # This function creates a string like col1 ilike '%generic_drug%' OR col2 ilike '%generic_drug%' for 5 columns
    make_or_for_case_statement <- function(drug_list_df, grouping_col, drug_names_col, string){
        groups = get(drug_list_df)[get(drug_list_df)[grouping_col] == string, drug_names_col]
        vec = pull(unique(groups))
        statement <- glue::glue("{col1} OR {col2}  OR {col3} OR {col4} OR {col5}",
                                col1 = paste0('generic_drug_package_descriptions', " ILIKE ", "'%", vec, "%'", collapse = ' OR ')
                                , col2 = paste0('std_drug_desc', " ILIKE ", "'%", vec, "%'", collapse = ' OR ')
                                , col3 = paste0('snomed_drug_descriptions', " ILIKE ", "'%", vec, "%'", collapse = ' OR ')
                                , col4 = paste0('ingredient_descriptions', " ILIKE ", "'%", vec, "%'", collapse = ' OR ')
                                , col5 = paste0('brand_name_descriptions', " ILIKE ", "'%", vec, "%'", collapse = ' OR '))

        return(statement)
    }

    # This function takes the fuction above and put the results in a case when statement like this
    # CASE WHEN (results from above fucnition) THEN 1 ELSE 0 END AS column. Multiple case when will be created as needed
    make_case_statement <- function(){
        groups = get(drug_list_df)[grouping_col]
        str_vec = pull(unique(groups))
        statement <- sapply(str_vec, function(str_vec) {
            paste0('CASE WHEN (',
                   make_or_for_case_statement(drug_list_df, grouping_col, drug_names_col, str_vec),
                   ') THEN 1 ELSE 0 END AS ',
                   str_vec, collapse = ', ')}
        )

        return(paste0(statement, collapse = ' ,'))
    }

    # The fuction writes a select max statement like this - MAX(column) AS column.
    select_max <- function(){
        groups = get(drug_list_df)[grouping_col]
        vec = as.list(unique(groups))

        statement <- paste0('MAX(', unlist(vec, use.names = F), ') AS ', unlist(vec, use.names = F), collapse = ', ')
        return(statement)}

    # Results from all the fuctions above a placed into this query

    sql3 <- glue::glue("
                       SELECT explorys_patient_id {group_var},
                       {select_max_statement}
                       FROM
                       (SELECT DISTINCT explorys_patient_id {group_var},
                       {case}
                       FROM {sandbox}.temp_meds

                       WHERE {date_condition} ) as A
                       GROUP BY explorys_patient_id {group_var}",
                       case = make_case_statement(),
                       select_max_statement = select_max(),
                       date_condition = date_condition,
                       sandbox = sandbox)

    results = dbGetQuery(sm, sql3)

    return(results)
}

# med_use_cancer = get_drugs_string(supermart = 'supermart_472',
#                                   sandbox = 'SANDBOX_GENESIS_BMS_COVID',
#                                   cohort = 'cohort_pts',
#                                   drug_list_df = 'explore_drug_list',
#                                   grouping_col = 'drug_group',
#                                   drug_names_col = 'drug_name',
#                                   index_col = 'index_dt',
#                                   date_condition = '(DATE(prescription_date) >= index_dt)'
#                                   , extra_grouping_vars = "pt_group"
#                                   )
#
#
# supermart = 'supermart_472'
# sandbox = 'SANDBOX_GENESIS_BMS_COVID'
# cohort = 'cohort_pts'
# drug_list_df = 'explore_drug_list'
# grouping_col = 'drug_group'
# drug_names_col = 'drug_name'
# index_col = 'index_dt'
# date_condition = '(DATE(prescription_date) >= index_dt)'
# extra_grouping_vars = NULL
#
#
# explore_drug_list = readxl::read_excel('Y:/genesis-research_global/BMS_COVID/data/BMS_COVID_Project_Codes_5Oct2020_V2_revised.xlsx', sheet = 'drug_list',
#                                        col_names = TRUE) %>%
#     mutate(drug_group = str_to_lower(drug_group))

