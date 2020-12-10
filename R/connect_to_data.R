#' Allows you to easily  connect to the database
#'
#' Takes in the username and password as strings
#' @param username Username as a string
#' @param password Password as a string
#' @return Returns an a connect object called sm. Refer to the object as sm. ie assign the results of the function to an object called sm
#' @export
connect_to_data <- function(username, password){

  smDriver = RJDBC::JDBC(driverClass = 'com.vertica.jdbc.Driver',
                  classPath = "C:\\Program Files\\Vertica Systems\\JDBC\\vertica-jdbc-7.2.2-0.jar")

  sm = RJDBC::dbConnect(smDriver, "jdbc:vertica://vertica-als-lb.explorys.net:5433/db1", username, password)

  assign("sm", sm, envir = .GlobalEnv)
  message("A JDBC connection called sm has been created in your gloabl environment")

}
