args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop("At least two arguments must be supplied\n", call. = FALSE)
}
datadir <- args[1]
destdatadir <- args[2]


datasets <- c(
  "sia-pa",
  "sih-sp",
  "cnes-pf",
  "sih-rd",
  "sia-aq",
  "sinasc",
  "ciha",
  "sia-ad",
  "cnes-st",
  "sim-do-cid10",
  "sia-ps",
  "cnes-eq",
  "cnes-sr",
  "sinan-deng",
  "sia-atd",
  "sih-rj",
  "sia-an",
  "sim-do-cid09",
  "sia-ar",
  "sim-do-cid10-preliminar",
  "cnes-ep",
  "sih-er",
  "sisprenatal-pn",
  "sim-doext-cid10",
  "cih-cr",
  "sinan-viol",
  "sinas-preliminar",
  "sinan-antr-preliminar",
  "sim-doinf-cid10",
  "cnes-lt",
  "cnes-hb",
  "sinan-anim",
  "sinan-tube",
  "sim-dofet-cid10",
  "sia-sad",
  "sim-doinf-cid09",
  "sinan-deng-preliminar",
  "sinan-iexo",
  "cnes-dc",
  "sinan-hans",
  "sinan-acgr",
  "sinan-viol-preliminar",
  "sinan-anim-preliminar",
  "sim-doext-cid09",
  "sinan-iexo-preliminar",
  "sinan-chik",
  "sinan-acgr-preliminar",
  "cnes-rc",
  "sinan-hepa-preliminar",
  "sinan-meni",
  "sia-abo",
  "sinan-acbi",
  "sim-dofet-cid09",
  "sinan-sifa-preliminar",
  "sia-acf",
  "sinan-tube-preliminar",
  "sia-ab",
  "cnes-in",
  "sinan-ltan",
  "sim-doext-cid10-preliminar",
  "sinan-meni-preliminar",
  "sinan-acbi-preliminar",
  "sinan-lept",
  "sinan-sifg-preliminar",
  "sinan-sifc-preliminar",
  "sinan-leiv",
  "sinan-hans-preliminar",
  "sinan-coqu",
  "sinan-zika",
  "sinan-chik-preliminar",
  "sinan-esqu",
  "sinan-lerd",
  "sim-domat-cid10",
  "sim-doinf-cid10-preliminar",
  "cnes-gm",
  "sinan-lept-preliminar",
  "sinan-chag",
  "sim-dofet-cid10-preliminar",
  "base-populacional-ibge-popt",
  "resp",
  "sinan-hant",
  "sinan-fmac",
  "sinan-lerd-preliminar",
  "sinan-mala",
  "cnes-ef",
  "sinan-esqu-preliminar",
  "sinan-fmac-preliminar",
  "cnes-ee",
  "sinan-pfan",
  "sinan-ment",
  "sinan-zika-preliminar",
  "sinan-mala-preliminar",
  "sinan-derm",
  "sinan-pair",
  "sinan-ment-preliminar",
  "sinan-fama",
  "sinan-hant-preliminar",
  "sim-domat-cid10-preliminar",
  "sinan-teta",
  "sinan-chag-preliminar",
  "sinan-pneu",
  "sinan-pair-preliminar",
  "sinam-derm-preliminar",
  "sinan-canc-preliminar",
  "sinan-pneu-preliminar",
  "sinan-canc",
  "sinan-ftif",
  "sinan-botu",
  "sinan-pfan-preliminar",
  "sinan-teta-preliminar",
  "sinan-dift",
  "sinan-raiv-preliminar",
  "sinan-ftif-preliminar",
  "sinan-coqu-preliminar",
  "sinan-pest",
  "sinan-raiv",
  "sinan-botu-preliminar",
  "sinan-cole",
  "sinan-tetn",
  "sinan-pest-preliminar",
  "sinan-dift-preliminar",
  "sinan-cole-preliminar",
  "sinan-tetn-preliminar"
)


for (datasetname in datasets) {
  datasetdir <- file.path(datadir, datasetname)
  datasetdestdir <- file.path(destdatadir, datasetname)

  # Create destination directory if not exists
  if (!dir.exists(datasetdestdir)) {
    dir.create(datasetdestdir, recursive = TRUE)
  }

  # Get list of files in data set directory
  files <- list.files(datasetdir, pattern = ".*\\.dbc", recursive = TRUE)

  for (f in files) {
    # Build destination file path
    destfilepath <- fs::path_ext_set(
      file.path(
        datasetdestdir,
        fs::path_rel(f, start = datasetdir)
      ),
      "parquet"
    )

    if (!dir.exists(dirname(destfilepath))) {
      # Create directory if it doesn't exists
      dir.create(dirname(destfilepath))
    } else if (file.exists(destfilepath)) {
      # Skip loop if file exists
      next
    }

    # Read DBC files
    d <- tibble::tibble(read.dbc::read.dbc(f, as.is = TRUE))

    # Write parquet file
    arrow::write_parquet(d, destfilepath)
  }
}
