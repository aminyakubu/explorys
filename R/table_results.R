#' Easily get results presentable for excel
#' This function allows you to get results that is easily presented in excel table shells. Usually, one may want
#' results of counts of a list of diagnoses. The dataset that goes into this function has a list of variables that
#' flag the whether a person has the event or not. Recommended for use for datasets derived using function like `get_diagnosis` and
#' `get_drugs_ndc` and similar functions from the explorys package.
#'
#' @param df Datafram with the flaging variables (1 or 0). Usually dataframe from functions like `get_diagnosis` among others from explorys
#' package
#' @ levels Specify the levels to be output. This takes a list. If you want all levels you can specify c(0,1,'Missing')
#' @export
#'
table_results = function(df, levels){

    for (i in 2:ncol(df)) {
        df[,i] = forcats::fct_explicit_na(factor(df[,i], levels = 0:1), na_level = "Missing")
    }

    var = names(df)
    output = vector('list', length = length(var))

    for (i in 2:length(df)) {
        output[[i]] = df %>% count(get(var[[i]]), .drop = FALSE) %>%
            mutate(prop = n/nrow(df)) %>%
            mutate(variable = var[[i]])  }

    fulltable = do.call(rbind, output) %>%
        rename(var_type = `get(var[[i]])`) %>%
        filter(var_type %in% levels) %>%
        select(variable, var_type, n, prop)

    return(fulltable)
}
