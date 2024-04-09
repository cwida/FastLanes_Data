This repository contains processed `.csv` files from the [NextiaJD dataset](https://homepages.cwi.nl/~boncz/NextiaJD/) and `.ipynb`/`.py` that we used to process them. 
* The files are stored in folders named identically to the respective CSV files. 
	* All ` `, `-` and `.` in file names are replaced by `_` to avoid problems when using files in C++.
* Each file in repo was originally smaller than 100MB and contains exactly 65,536 (64 * 1024) rows of data for uniformity. 
* To maintain consistency across the dataset, we have replaced various file-specific delimiters with a single '|' delimiter. 
* All the data is in UTF-8 encoding. 
* During processing, some files were identified to have corrupted entries or columns that required cleaning. The sections below detail the specific issues encountered and the corrective actions taken for selected files. All these changes are reflected in our code in `get-data.ipynb` (and `get-data.py`), but sometimes not in the data we provide, as corrupted entries may occur only after the 64K rows.

## Chicago Crimes (2001 to 2004)
In the `Chicago_Crimes_2001_to_2004.csv` file, we encountered issues in row 1,602,849, particularly with the 'Y Coordinate', 'Year', and 'Updated On' columns. The 'Y Coordinate' column contained `None` values. To address this, we replaced the faulty 'Y Coordinate' entry in row 1,602,849 with an empty value, while the 'Year' and 'Updated On' entries were corrected using the data from row 1,602,848.

## Chicago Crimes (2005 to 2007)
The `Chicago_Crimes_2005_to_2007.csv` file had a problematic 'Location' entry at row 533,719. Given that the location can be inferred from the 'Longitude' and 'Latitude' columns, we manually corrected this issue to ensure data integrity.

## Arizona Postcodes
In the `az.csv` file, the “POSTCODE” column occasionally contains spaces (" "), which we interpreted as NaN values. To maintain data type consistency, we replaced these spaces with empty values. Additionally, we identified and corrected invalid string entries in the postcodes at rows 423,531, 2,561,616, and 2,572,688 by setting them to empty values too.

# Future work
## Schema inference
We only infer the schema after stripping the data to 64 * 1024 rows. Because of that, some columns (like the `congestion_surcharge` column of `yellow_tripdata_2019_01`) contain only null values. By default, our pipeline will infer that this column is of VARCHAR type. It then needs to be changed manually to the actual type of the column. Right now, there is no mechanism of alerting when column contains only nones, so if more data is added in future, there can be wrong inferences that we are not aware of. Implement it.

## Business Licenses
The `business-licences.csv` file's “House” column is treated as VARCHAR. However, it could potentially be stored as a more compressible data type with due cleaning.

## US Permanent Visas
The `us_perv_visas.csv` file presented a challenge with its messy data. Currently, almost all columns are treated as VARCHARs. Through meticulous cleaning, it is possible to have more compressible data types.