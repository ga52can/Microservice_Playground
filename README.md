#Microservices

+ Total number of microservices - 9
+ Service starting order -
    * Config Service
    * Eureka Service
    * Zuul Service (Not necessary)
    * All other services.
+ Service Dependency
    * Config Service - All services depend on this
    * Zuul - All services depend on this except config and eureka
    * Eureka Service - Business core and zuul depend on this
    * Maps Service - TravelCompanion, DriveNow and DeutscheBahn depend on this
    * Accounting Service - TravelCompanion, Drivenow and DeutscheBahn depend on this
+ All authentication is handled through zuul. All other services are NOT secured if not reached through zuul.
+ To logout visit ip:<zuul_port>/logout
+ DB Schema is defined in create table commands in the db_setup.sql
+ DB passwords are defined inside the resources of the config server ( in the application.properties)
+ Accounting service does not know which user the request is coming from. For cross microservice user auth we will need ticketing which is possible through oauth but is not implemented as of now.