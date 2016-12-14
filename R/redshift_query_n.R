#' @name redshift_query_n
#' @title redshift_query_n
#' @param sql.string  SQL Query string
#' @param conn  Redshift database connection object
#' @param bucket  AWS bucket to store unload files
#' @param transform.function function object detailing additional transformations to perform on the data
#' @param parallel TRUE or FALSE, do you want to do parallel processing.  Defaults to FALSE
#' @param cores  Cores to utilise, defaults to NULL, if NULL will detect cores on machine and utilise all of them.  If parallel = FALSE parameter is not needed.
#' @param package.list packages  packages needed in the transformation function.  If parallel = FALSE parameter is not needed.  NB:  If parallel = FALSE the package must be loaded in the local environment.
#' @description Retrieves unloaded redshift query components from an AWS library.  The process is parallelised to speed it up.  This means much larger datasets can be queried and compiled.  If parallel = FALSE parameter is not needed.
#' @examples
#'
#' # Firstly you'll need to set up AWS permissions
#'
#' Sys.setenv("AWS_ACCESS_KEY_ID" = "key",
#'            "AWS_SECRET_ACCESS_KEY" = "key",
#'            "AWS_DEFAULT_REGION" = "key")
#'
#' # Define SQL string for database query
#' # e.g.
#'
#' sql.string <- "SELECT * FROM some.table"
#'
#' # Setup a database connection object using redshift.connect
#'
#' conn <- redshift.connect("connection string")
#'
#' # You'll also need an existing bucket where you can deposit interim files
#' # e.g.
#'
#' bucket <- "an-aws-bucket"
#'
#' # A transformation function used to make additional alterations on the data
#'
#' transform.function <- function(x) {data.table(x)}
#'
#' # Decide if you want to do parallel processing, if no
#'
#' parallel <- FALSE
#'
#' # if yes
#'
#' parallel <- TRUE
#'
#' # If parallel = TRUE
#' # Define number of threads, if undefined it'll detect the number of cores on the system and use these.
#'
#' cores <- 10
#'
#' # If parallel = TRUE
#' # A list of packages that must be used in the transformation function
#' # e.g.
#'
#' package.list <- "data.table"
#'
#' # Execute the function
#'
#' data <- redshift_query_n(sql.string, conn, bucket, cores, transform.function, package.list)
#'
#' @export


redshift_query_n <- function(sql.string, conn, bucket, transform.function = NULL, parallel = FALSE, cores = NULL, package.list = NULL) {


  execute_redshift_query <- function(sql.string) {

    rand.prefix <- stri_rand_strings(1, 20)


    key.prefix <- paste0("parallel-query-dump/output/",rand.prefix,"/")

    redshift.unload(conn, sql.string,
                    filename = paste0('s3://', bucket, '/', key.prefix),
                    delim = ",", zip = F,
                    aws.role = 'arn:aws:iam::136393635417:role/RedshiftCopyUnload')

    entries <- aws.s3::get_bucket(bucket, key.prefix)

    keys <- unlist(llply(seq(6,NROW(entries),1), function(x) {entries[[x]]$Key}))

    return(keys)}

  keys <- execute_redshift_query(sql.string)

  readDataFromS3 <- function(key) {

    cat(sprintf('reading from bucket %s key %s\n', bucket, key))

    obj <- aws.s3::get_object(key, bucket)
    read.csv(text = rawToChar(obj), header = FALSE, stringsAsFactors = FALSE)

  }

  if(parallel == TRUE) {cores.n <- if(is.null(cores)) {detectCores()} else {cores}}

  if(parallel == TRUE) {cluster <- makeCluster(cores)}

  if(parallel == TRUE) {registerDoParallel(cluster)}

  if(parallel == TRUE) {cat(sprintf('Made cluster with %d cores\n', cores.n))}



  if(parallel == TRUE) {

  data <- tryCatch(if(is.null(transform.function))

       {rbindlist(foreach(key = keys, .packages = package.list) %dopar% readDataFromS3(key))} else

       {rbindlist(foreach(key = keys, .packages = package.list) %dopar% transform.function(readDataFromS3(key)))},

        error = function(err) {print("Data binding failed.")})

       } else {

  data <- tryCatch(if(is.null(transform.function))

        {rbindlist(llply(keys, readDataFromS3(key)))} else

        {rbindlist(llply(keys, function(x) {transform.function(readDataFromS3(x))}))},

        error = function(err) {print("Data bindling failed")})

       }


  print("Cleaning up")

  if(parallel == TRUE) {

  foreach(key = keys, .packages = "aws.s3") %dopar% delete_object(key, bucket, quiet = TRUE)

  stopCluster(cluster) } else {

  for (i in 1:NROW(keys)) {

    delete_object(keys[i], bucket, quiet = TRUE)

  }

  }

  return(data)}

