This repository contains processed `.csv` files from the NextiaJD dataset, available at [the NextiaJD dataset](https://homepages.cwi.nl/~boncz/NextiaJD/). These files are stored in folders named identically to the respective CSV files. We have ensured that each file is smaller than 100MB and contains exactly 65,536 (64 * 1024) rows of data for uniformity. To maintain consistency across the dataset, we have replaced various file-specific delimiters with a single '|' delimiter. During processing, some files were identified to have corrupted entries or columns that required cleaning. The sections below detail the specific issues encountered and the corrective actions taken for selected files. All these changes are reflected in our code in `get-data.ipynb`, but not in the data we provide, as corrupted entries occur only after the 64K rows.

## Chicago Crimes (2001 to 2004)


In the `Chicago_Crimes_2001_to_2004.csv` file, we encountered issues in row 1,602,849, particularly with the 'Y Coordinate', 'Year', and 'Updated On' columns. The 'Y Coordinate' column contained `None` values. To address this, we replaced the faulty 'Y Coordinate' entry in row 1,602,849 with an empty value, while the 'Year' and 'Updated On' entries were corrected using the data from row 1,602,848.

## Chicago Crimes (2005 to 2007)


The `Chicago_Crimes_2005_to_2007.csv` file had a problematic 'Location' entry at row 533,719. Given that the location can be inferred from the 'Longitude' and 'Latitude' columns, we manually corrected this issue to ensure data integrity.

## Arizona Postcodes


In the `az.csv` file, the “POSTCODE” column occasionally contains spaces (" "), which we interpreted as NaN values. To maintain data type consistency, we replaced these spaces with empty values. Additionally, we identified and corrected invalid string entries in the postcodes at rows 423,531, 2,561,616, and 2,572,688 by setting them to empty values too.

# Future work


## Business Licenses


The `business-licences.csv` file's “House” column is treated as VARCHAR. However, it could potentially be stored as a more compressible data type with due cleaning.

## US Permanent Visas


The `us_perv_visas.csv` file presented a challenge with its messy data. Currently, almost all columns are treated as VARCHARs. Through meticulous cleaning, it is possible to have more compressible data types.
