#' Returns a data frame with the components of file names parsed
parse.filenames <- function(files) {
  file_stem <- fs::path_ext_remove(basename(files))
  file_parts <- stringr::str_split(file_stem, "_")
  datasetname <- unlist(lapply(file_parts, `[[`, 1))
  partitions <- lapply(file_parts, `[[`, 2)
  partitions <- stringr::str_split(partitions, "-")
  date <- unlist(lapply(partitions, `[[`, 1))
  uf <- unlist(lapply(partitions, `[[`, 2))
  updated_at <- unlist(lapply(file_parts, `[[`, 3))
  data.frame(cbind(datasetname, filepath = files, date, uf, updated_at))
}


get.files.year <- function(datadir, datasetname, year) {
  pattern <- stringr::str_interp(
    "^${datasetname}_$[04d]{year}.*.dbc$")
  files <- list.files(
    file.path(datadir, datasetname),
    pattern = pattern,
    full.names = TRUE
  )
  parse.filenames(files)
}


get.files.yearmonth <- function(datadir, datasetname, year, month) {
  pattern <- stringr::str_interp(
    "^${datasetname}_$[04d]{year}$[02d]{month}.*.dbc$")
  files <- list.files(
    file.path(datadir, datasetname),
    pattern = pattern,
    full.names = TRUE
  )
  parse.filenames(files)
}


get.files <- function(datasetdir) {
  files <- list.files(datasetdir, full.names = TRUE)
  parse.filenames(files)
}
