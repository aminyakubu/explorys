#' Get demographic characteristics of patients in explorys database
#'
#' This function takes in the cohort with ids and outputs the demographic characteristics like gender, birth year and
#' insurance, race/ethnicity. It assumes the cohort has id variable explorys_patient_id.
#' Always make sure thisse variable exists with the exact same name.
#' The results are aggregated from the entire history of patients existence in the database. So for example if a patient reports
#' Medicare as their insurance at some point in time and later reports Medicaid, they will be in both insurance categories as the demographic table
#' doesn't incorporate (not possible to tell when the insurance information was recorded)
#'
#' @param supermart Name of the supermart where the data is stored. Eg. supermart_392
#' @param sandbox Name of the writable location usually sandbox where cohort is saved
#' @param cohort Name of the cohort with explorys_patient_id's
#' @return A dataframe with flags of counts/data of demographic variables
#' @export

get_demographics = function(supermart, sandbox, cohort){

    sql0 = glue::glue("SELECT DISTINCT d.explorys_patient_id, std_gender, birth_year, death_year, postal_code_3, std_ethnicity, std_insurance_type, std_race
                     FROM {supermart}.v_demographic AS d
                      INNER JOIN {sandbox}.{cohort} AS s
                      ON d.explorys_patient_id = s.explorys_patient_id",
                      supermart = supermart,
                      cohort = cohort)

    demos = dbGetQuery(sm, sql0)

    demos01 = demos %>% group_by(explorys_patient_id) %>%
        summarize(
            std_gender1 = paste(unique(std_gender), collapse = ','),
            birth_year1 = paste(unique(birth_year), collapse = ','),
            death_year1 = paste(unique(death_year), collapse = ','),
            postal_code_3_1 = paste(unique(postal_code_3), collapse = ','),
            std_ethnicity1 = paste(unique(std_ethnicity), collapse = ','),
            std_insurance_type1 = paste(unique(std_insurance_type), collapse = ','),
            std_race1 = paste(unique(std_race), collapse = ','))

    # Replace NAs with empty string
    demos02 = demos01 %>%
        mutate(birth_year1 = str_replace_all(birth_year1, ',NA', ''),
               death_year1 = str_replace_all(death_year1, 'NA', ''),
               postal_code_3_1 = str_replace_all(postal_code_3_1, 'NA,|,NA|NA', ''),
               std_ethnicity1 = str_replace_all(std_ethnicity1, 'NA,|,NA|NA', ''),
               std_insurance_type1 = str_replace_all(std_insurance_type1, 'NA,|,NA|NA', ''),
               std_race1 = str_replace_all(std_race1, 'NA,|,NA|NA', '')) %>%
        mutate(birth_year1= as.numeric(birth_year1))

    pts = demos02 %>%
    mutate(std_insurance_type1 = str_replace_all(std_insurance_type1, 'OTHERPUBLIC', 'otherpublic'),

           # Patient could have multiple
           insurance_medicare = if_else(str_detect(std_insurance_type1, 'MEDICARE'), 'Yes', ''),
           insurance_medicaid = if_else(str_detect(std_insurance_type1, 'MEDICAID'), 'Yes', ''),
           insurance_private = if_else(str_detect(std_insurance_type1, 'PRIVATE'), 'Yes', ''),
           insurance_selfpay = if_else(str_detect(std_insurance_type1, 'SELFPAY'), 'Yes', ''),
           insurance_otherpublic = if_else(str_detect(std_insurance_type1, 'otherpublic'), 'Yes', ''),
           insurance_other = if_else(str_detect(std_insurance_type1, 'OTHER'), 'Yes', ''),
           insurance_unk = if_else(str_detect(std_insurance_type1, 'UNKNOWN'), 'Yes', ''),
           insurance_research = if_else(str_detect(std_insurance_type1, 'RESEARCH'), 'Yes', ''),
           insurance_missing = if_else(std_insurance_type1 == '', 'Yes', ''),

           # Let's remove declined as an option if we already know their ethnicity from a different time. Same with unknown.

           ethnicity = if_else(str_length(std_ethnicity1) > 8 & str_detect(std_ethnicity1, 'declined'),
                               str_replace(std_ethnicity1, ',declined|declined,',''), std_ethnicity1),
           ethnicity_final = if_else(str_length(ethnicity) > 7 & str_detect(ethnicity, 'unknown'),
                                     str_replace(ethnicity, ',unknown|unknown,',''), ethnicity),
           ethnicity_final1 = str_replace_all(ethnicity_final, 'non_hispanic', 'NON_HISPANIC'),

           # Let's categorize ethnicity
           ethn_hispanic = if_else(str_detect(ethnicity_final1, 'hispanic'), 'Yes', ''),
           ethn_non_hispanic = if_else(str_detect(ethnicity_final1, 'NON_HISPANIC'), 'Yes', ''),
           ethn_unknown = if_else(str_detect(ethnicity_final1, 'unknown'), 'Yes', ''),
           ethn_other = if_else(str_detect(ethnicity_final1, 'other'), 'Yes', ''),
           ethn_declined = if_else(str_detect(ethnicity_final1, 'declined'), 'Yes', ''),
           ethn_missing = if_else(ethnicity_final1 == '', 'Yes', '')) %>%

    # Let's separate race

    separate(std_race1, into = c("race1", "race2","race3","race4","race5"), sep = (','))


# Let's split the zips into regions

    region = pts %>%
        select(explorys_patient_id, postal_code_3_1) %>%
        separate(postal_code_3_1, into = c("zip1", "zip2","zip3"), sep = (',')) %>%
        gather(key = 'zip', value = 'zip3', zip1:zip3) %>%
        select(-zip) %>%
        filter(!is.na(zip3))

    reg_crxwalk = get(load(file = "data/reg_crxwalk.rda")) %>%
        select(zip, city, state_id, state_name) %>%
        mutate(zip3 = substr(zip, 1, 3)) %>%
        distinct(zip3, state_id, state_name) %>%
        mutate(
        region = dplyr::case_when(
            state_id %in% c("CT", "ME", "MA", "NH", "RI","VT", "NJ", "NY", "PA") ~ "Region_Northeast",
            state_id %in% c("IN", "IL", "MI", "OH", "WI", "IA", "KS", "MN", "MO",
                            "NE", "ND", "SD") ~ "Region_Midwest",
            state_id %in% c("DE", "DC", "FL", "GA", "MD", "NC", "SC", "VA", "WV",
                            "AL", "KY", "MS", "TN", "AR", "LA", "OK", "TX") ~ "Region_South",
            state_id %in% c("AZ", "CO", "ID", "NM", "MT", "UT", "NV", "WY", "AK",
                            "CA", "HI", "OR", "WA") ~ "Region_West",
            state_id == "PR" ~ "Region_Puerto_Rico"))

    reg_match = merge(region, reg_crxwalk, by = 'zip3', all.x = T) %>%
    select(explorys_patient_id, region) %>%
    filter(!is.na(region)) %>%
    mutate(reg_val = 'Yes') %>%
    distinct() %>%
    spread(key = region, value = reg_val) %>%
    janitor::clean_names()


    race_raw = pts %>% select(explorys_patient_id, starts_with('race')) %>%
    gather(key = 'race', value = 'std_code', race1:race5) %>%
    select(-race) %>%
    filter(!is.na(std_code))

    race_desc = dbGetQuery(sm, "SELECT * FROM xref.Std_code_value WHERE attribute = 'race' ")

# Let's merge by race code

    race_data = merge(race_raw, race_desc, by = 'std_code') %>%
        select(explorys_patient_id, std_value) %>%
        mutate(race_val = 'Yes') %>%
        spread(key = std_value, value = race_val) %>%
        janitor::clean_names() %>%
        select_all(.funs = funs(paste0("race_",.))) %>%
        rename(explorys_patient_id = race_explorys_patient_id) %>%

    # We are double counting patients here (someone could be african american and asian at the same time). But if we have any info, then the person won't be classified as unknown
    # or declined.
        mutate(race_unk = if_else(is.na(race_african_american) & is.na(race_asian) & is.na(race_caucasian) & is.na(race_hispanic_latino) & is.na(race_multi_racial) & is.na(race_other) & !is.na(race_unknown), race_unknown, NULL)) %>%

        mutate(race_true_rtc = if_else(is.na(race_african_american) & is.na(race_asian) & is.na(race_caucasian) & is.na(race_hispanic_latino) & is.na(race_multi_racial) & is.na(race_other) & is.na(race_unk) & !is.na(race_refused_to_classify), race_refused_to_classify, NULL)) %>%
        select(-race_unknown, -race_refused_to_classify)

    sql2 = glue::glue("SELECT DISTINCT d.explorys_patient_id, d.death_year, d.is_deceased
                      FROM {sandbox}.{cohort} AS c
                      INNER JOIN {supermart}.v_demographic AS d
                      ON c.explorys_patient_id = d.explorys_patient_id
                      WHERE is_deceased = true ",
                      supermart = supermart,
                      sandbox = sandbox,
                      cohort = cohort)

    mortality = dbGetQuery(sm, sql2)

    mortality01 = mortality %>%
        mutate(has_death_yr = if_else(!is.na(death_year), 'Yes', NULL)) %>%
        merge(., pts, by = 'explorys_patient_id') %>%
        select(explorys_patient_id, has_death_yr, is_deceased)



# Now we can merge this dataset back to the original
    final = merge(pts, race_data, by = 'explorys_patient_id', all.x = T)

    final01 = merge(final, reg_match, by = 'explorys_patient_id', all.x = T) %>%
        merge(., mortality01, by = 'explorys_patient_id', all.x = T)

    final01$race_unk[which(final$race_unk == 0)] = NA
    final01$race_true_rtc[which(final$race_true_rtc == 0)] = NA

    final02 = final01 %>%
    mutate(race_unk = as.character(race_unk),
           true_rtc_race = as.character(race_true_rtc)) %>%
    mutate_all(list(~ na_if(., ""))) %>%
    mutate(race_missing = if_else(is.na(race_african_american) & is.na(race_asian) & is.na(race_caucasian) & is.na(race_hispanic_latino) & is.na(race_multi_racial) & is.na(race_other) & is.na(race_unk) & is.na(race_true_rtc), 'Yes', ''),

           region_missing = if_else(is.na(region_midwest) & is.na(region_northeast) & is.na(region_puerto_rico) & is.na(region_south) & is.na(region_west), 'Yes', '')) %>%
    mutate_all(list(~ na_if(., ""))) %>%
    select(explorys_patient_id, birth_year1, std_gender1, is_deceased, has_death_yr, death_year1,
           ethn_hispanic, ethn_non_hispanic, ethn_other, ethn_declined, ethn_unknown, ethn_missing,
           race_african_american, race_asian, race_caucasian, race_hispanic_latino,  race_multi_racial, race_other, race_unk, race_true_rtc, race_missing,
           insurance_medicare, insurance_medicaid, insurance_private, insurance_selfpay, insurance_otherpublic, insurance_other, insurance_unk, insurance_research, insurance_missing,
           region_northeast, region_midwest, region_south, region_west, region_puerto_rico, region_missing)

    return(final02)

}

# library(explorys)
# library(RJDBC)
# library(magrittr)
#library(tidyselect)
#library(tidyr)
#library(dplyr)
#library(stringr)

#supermart = "supermart_472"
#sandbox = "SANDBOX_GENESIS_BMS_COVID"
#cohort = "cohort_pts"

 #demos_covid = get_demographics("supermart_472",
 #                               "SANDBOX_GENESIS_BMS_COVID",
 #                               "cohort_pts")





