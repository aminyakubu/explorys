#' Get demographic characteristics of patients in explorys database
#'
#' This function takes in the cohort with ids and index date and outputs the demographic characteristics like gender, age and 
#' and groups, insurance, race/ethnicity. It assumes the cohort has variables called index_dt and explorys_patient_id. 
#' Always make sure those 2 variables exists with the exact same names.
#' The results are aggregated from the entire history of patients existence in the database. So for example if a patient reports 
#' Medicare as their insurance at some point in time and later reports Medicaid, they will be counted in both categories. 
#'
#' @param supermart Name of the supermart where the data is stored. Eg. supermart_392
#' @param sandbox Name of the writable location usually sandbox where cohort is saved
#' @param cohort Name of the cohort with explorys_patient_id's and index dates. Must be called index_dt
#' @return A dataframe with flags of counts of demographic variables
#' @export

get_demographics = function(supermart, sandbox, cohort){
 
    sql0 = glue::glue("SELECT DISTINCT d.explorys_patient_id, std_gender, birth_year, death_year, postal_code_3, std_ethnicity, std_insurance_type, std_race, index_dt
                     FROM {supermart}.v_demographic AS d
                      INNER JOIN {sandbox}.{cohort} AS s
                      ON d.explorys_patient_id = s.explorys_patient_id", 
                      supermart = supermart, 
                      cohort = cohort)
    
    age = dbGetQuery(sm, sql0) 
    
    age01 = age %>% group_by(explorys_patient_id) %>% 
        summarize(
            std_gender1 = paste(unique(std_gender), collapse = ','),
            birth_year1 = paste(unique(birth_year), collapse = ','),
            death_year1 = paste(unique(death_year), collapse = ','),
            postal_code_3_1 = paste(unique(postal_code_3), collapse = ','),
            std_ethnicity1 = paste(unique(std_ethnicity), collapse = ','),
            std_insurance_type1 = paste(unique(std_insurance_type), collapse = ','),
            std_race1 = paste(unique(std_race), collapse = ','),
            index1 = paste(unique(index_dt), collapse = ','))
    
    # Replace NAs with empty string
    age02 = age01 %>% 
        mutate(birth_year1 = str_replace_all(birth_year1, ',NA', ''),
               death_year1 = str_replace_all(death_year1, ',NA', ''),
               postal_code_3_1 = str_replace_all(postal_code_3_1, 'NA,|,NA|NA', ''),
               std_ethnicity1 = str_replace_all(std_ethnicity1, 'NA,|,NA|NA', ''),
               std_insurance_type1 = str_replace_all(std_insurance_type1, 'NA,|,NA|NA', ''),
               std_race1 = str_replace_all(std_race1, 'NA,|,NA|NA', '')) %>% 
        mutate(birth_year1= as.numeric(birth_year1),
               index_year = lubridate::year(as.Date(index1))) %>% 
        filter(!is.na(birth_year1)) %>% 
        mutate(age = index_year - birth_year1)
    
    pts = age02 %>% 
    mutate(age = index_year - birth_year1,
           age_cat = if_else(age < 18, '< 18',
                             if_else(age >= 18 & age <= 39, '18-39',
                                     if_else(age >= 40 & age <= 54, '40-54',
                                             if_else(age >= 55 & age <= 64, '55-64',
                                                     if_else(age >= 65 & age <= 74, '65-74',
                                                             if_else(age >= 75, '75+', 'CHECK')))))),
           
           std_insurance_type1 = str_replace_all(std_insurance_type1, 'OTHERPUBLIC', 'otherpublic'),
           
           # Patient could have multiple
           medicare = if_else(str_detect(std_insurance_type1, 'MEDICARE'), '1', ''),
           medicaid = if_else(str_detect(std_insurance_type1, 'MEDICAID'), '1', ''),
           private = if_else(str_detect(std_insurance_type1, 'PRIVATE'), '1', ''),
           selfpay = if_else(str_detect(std_insurance_type1, 'SELFPAY'), '1', ''),
           otherpublic = if_else(str_detect(std_insurance_type1, 'otherpublic'), '1', ''),
           other_insur = if_else(str_detect(std_insurance_type1, 'OTHER'), '1', ''),
           unk_insur = if_else(str_detect(std_insurance_type1, 'UNKNOWN'), '1', ''),
           research_insur = if_else(str_detect(std_insurance_type1, 'RESEARCH'), '1', ''),
           missing_insur = if_else(std_insurance_type1 == '', '1', ''),
           
           # Let's remove declined as an option if we already know their ethnicity from a different time. Same with unknown. 
           
           ethnicity = if_else(str_length(std_ethnicity1) > 8 & str_detect(std_ethnicity1, 'declined'),
                               str_replace(std_ethnicity1, ',declined|declined,',''), std_ethnicity1),
           ethnicity_final = if_else(str_length(ethnicity) > 7 & str_detect(ethnicity, 'unknown'), 
                                     str_replace(ethnicity, ',unknown|unknown,',''), ethnicity),
           ethnicity_final1 = str_replace_all(ethnicity_final, 'non_hispanic', 'NON_HISPANIC'),
           
           # Let's categorize ethnicity
           hispanic = if_else(str_detect(ethnicity_final1, 'hispanic'), '1', ''),
           non_hispanic = if_else(str_detect(ethnicity_final1, 'NON_HISPANIC'), '1', ''),
           unknown_ethn = if_else(str_detect(ethnicity_final1, 'unknown'), '1', ''),
           other_ethn = if_else(str_detect(ethnicity_final1, 'other'), '1', ''),
           declined_ethn = if_else(str_detect(ethnicity_final1, 'declined'), '1', ''),
           missing_ethn = if_else(ethnicity_final1 == '', '1', '')) %>% 
    
    # Let's separate race 
    
    separate(std_race1, into = c("race1", "race2","race3","race4","race5"), sep = (',')) 


# Let's split the zips into regions
    
    region = pts %>% 
        select(explorys_patient_id, postal_code_3_1) %>% 
        separate(postal_code_3_1, into = c("zip1", "zip2","zip3"), sep = (',')) %>%
        gather(key = 'zip', value = 'zip3', zip1:zip3) %>% 
        select(-zip) %>% 
        filter(!is.na(zip3))
    
    reg_crxwalk = load(file = "data/reg_crxwalk.rda") %>% 
        select(zip, city, state_id, state_name) %>% 
        mutate(zip3 = substr(zip, 1, 3)) %>% 
        distinct(zip3, state_id, state_name) %>% 
        mutate(
        region = dplyr::case_when(
            state_id %in% c("CT", "ME", "MA", "NH", "RI","VT", "NJ", "NY", "PA") ~ "Northeast",
            state_id %in% c("IN", "IL", "MI", "OH", "WI", "IA", "KS", "MN", "MO",
                            "NE", "ND", "SD") ~ "Midwest",
            state_id %in% c("DE", "DC", "FL", "GA", "MD", "NC", "SC", "VA", "WV",
                            "AL", "KY", "MS", "TN", "AR", "LA", "OK", "TX") ~ "South",
            state_id %in% c("AZ", "CO", "ID", "NM", "MT", "UT", "NV", "WY", "AK",
                            "CA", "HI", "OR", "WA") ~ "West",
            state_id == "PR" ~ "Puerto Rico"))
    
    reg_match = merge(region, reg_crxwalk, by = 'zip3', all.x = T) %>% 
    select(explorys_patient_id, region) %>% 
    filter(!is.na(region)) %>% 
    mutate(reg_val = '1') %>%
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
        mutate(race_val = '1') %>% 
        spread(key = std_value, value = race_val) %>% 
        janitor::clean_names() %>% 
    
    # We are double counting patients here (someone could be african american and asian at the same time). But if we have any info, then the person won't be classified as unknown
    # or declined. 
        mutate(unk_race = if_else(is.na(african_american) & is.na(asian) & is.na(caucasian) & is.na(hispanic_latino) & is.na(multi_racial) & is.na(other), as.numeric(unknown), 0)) %>% 

        mutate(true_rtc_race = if_else(is.na(african_american) & is.na(asian) & is.na(caucasian) & is.na(hispanic_latino) & is.na(multi_racial) & is.na(other) & (unk_race == 0 | is.na(unk_race)), as.numeric(refused_to_classify), 0)) %>% 
        select(-unknown, -refused_to_classify)
    
    
    fin_coh = pts %>% 
        select(explorys_patient_id, index1) %>% 
        mutate(index_yr = lubridate::year(as.Date(index1))) 
    
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
        mutate(has_death_yr = if_else(is.na(death_year), '0', '1', '0')) %>% 
        merge(., fin_coh, by = 'explorys_patient_id') %>% 
        mutate(death_aft_index = if_else(death_year >= index_yr, '1', '0', '0')) %>% 
        select(explorys_patient_id, death_aft_index, has_death_yr, is_deceased)



# Now we can merge this dataset back to the original 
    final = merge(pts, race_data, by = 'explorys_patient_id', all.x = T)
    
    final01 = merge(final, reg_match, by = 'explorys_patient_id', all.x = T) %>% 
        merge(., mortality01, by = 'explorys_patient_id', all.x = T)
    
    final01$unk_race[which(final$unk_race == 0)] = NA
    final01$true_rtc_race[which(final$true_rtc_race == 0)] = NA
    
    final02 = final01 %>% 
    mutate(unk_race = as.character(unk_race),
           true_rtc_race = as.character(true_rtc_race),
           index_year = as.character(index_year)) %>% 
    mutate_all(list(~ na_if(., ""))) %>% 
    mutate(missing_race = if_else(is.na(african_american) & is.na(asian) & is.na(caucasian) & is.na(hispanic_latino) & is.na(multi_racial) & is.na(other) & is.na(unk_race) & is.na(true_rtc_race), '1', ''),
           
           missing_region = if_else(is.na(midwest) & is.na(northeast) & is.na(puerto_rico) & is.na(south) & is.na(west), '1', '')) %>% 
    mutate_all(list(~ na_if(., "")))
    
    return(final02)
    
}



