import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import bz2
import duckdb
import yaml
import pandas as pd
import csv
import sys

url = 'https://homepages.cwi.nl/~boncz/NextiaJD/'
cwd = os.getcwd()


def is_downloadable(url):
    """
    Does the url contain a downloadable resource
    """
    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get('content-type', '')
    if 'text' in content_type.lower() or 'html' in content_type.lower():
        return False
    return True


def get_file_size(url):
    """
    Get the size of the file at the given URL in MB
    """
    response = requests.head(url, allow_redirects=True)
    size = int(response.headers.get('content-length', 0))
    return size / 1024 / 1024  # Convert to MB


def download_and_decompress_file(url, folder_path, csv_filename):
    """
    Download a bz2 compressed file from a given URL, decompress it, and save as CSV in its own folder
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = os.path.join(folder_path, csv_filename)
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        decompressed_data = bz2.decompress(response.content)
        with open(file_path, 'wb') as f:
            f.write(decompressed_data)
        print(f"Decompressed and saved {file_path}")
    else:
        print(f"Failed to download {url}")


def process_csv_file(csv_path, max_rows):
    """
    Reads a CSV file, retains only the first `max_rows` of data excluding the header,
    and rewrites the CSV file with this subset. If the actual number of data rows in the
    file is less than `max_rows`, prints a warning indicating the file is shorter than expected.

    Parameters:
    - csv_file_path (str): The path to the CSV file to process.
    - max_rows (int): The maximum number of data rows to keep in the CSV file, excluding the header.

    Returns:
    - None: This function directly modifies the file specified by `csv_file_path` and does not return a value.

    Side effects:
    - Rewrites the CSV file at `csv_file_path` with up to `max_rows` of data, preserving the header.
    - Prints a warning to the console if the file contains fewer data rows than `max_rows`.
    """
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            rows = list(reader)

        num_rows = len(rows) - 1
        if num_rows < max_rows:
            print(f"Warning: The file {csv_path} has only {num_rows} rows, which is less than {max_rows}.")

        # Write back only the desired number of rows (including the header)
        with open(csv_path, 'w') as file:
            writer = csv.writer(file)
            for row in rows[:max_rows + 1]:
                writer.writerow(row)

    except Exception as e:
        print(f"An error occurred while processing {csv_path}: {e}")


# ========================================================
# ========== Download the files that are <100MB ==========
# ========================================================

page = requests.get(url)
soup = BeautifulSoup(page.text, 'html.parser')
size_limit = 64 * 1024

# Find all links on the page
for link in soup.find_all('a'):
    file_url = urljoin(url, link['href'])

    if is_downloadable(file_url):
        size_mb = get_file_size(file_url)

        if size_mb < 100:
            original_filename = link['href']
            if original_filename.endswith('.bz2'):
                # Change the extension from .bz2 to .csv
                csv_filename = original_filename[:-4]
                folder_name = csv_filename.rsplit('.', 1)[0]
                folder_path = os.path.join(os.getcwd(), folder_name)

                download_and_decompress_file(file_url, folder_path, csv_filename)

print("Download and decompression completed for all files under 100MB.")


# =================================================================================================
# ========== Download metadata.csv and delete information about files we didn't download ==========
# =================================================================================================

response = requests.get(url)
response.raise_for_status()
soup = BeautifulSoup(response.text, 'html.parser')
link = soup.find('a', href=lambda href: href and 'metadata.csv' in href)

if link:
    file_url = urljoin(url, link['href'])
    file_response = requests.get(file_url)
    file_response.raise_for_status()
    with open('metadata.csv', 'wb') as file:
        file.write(file_response.content)
    print("The metadata.csv file has been downloaded successfully!")
else:
    print("The metadata.csv file was not found on the page.")

df = pd.read_csv('metadata.csv')
folders = [name for name in os.listdir('.') if os.path.isdir(name)]
valid_filenames = [f"{folder}.csv" for folder in folders]
df_filtered = df[df['filename'].apply(lambda x: any(x == valid_name for valid_name in valid_filenames))]
df_filtered.drop(axis=1, labels='file_size', inplace=True)


# ========================================================
# ========== Keep only the first 64 * 1024 rows ==========
# ========================================================

# Increase the maximum field size limit
csv.field_size_limit(sys.maxsize)

folders = [f for f in os.listdir(cwd) if os.path.isdir(os.path.join(cwd, f))]

for folder in folders:
    if folder == '.ipynb_checkpoints':
        continue
    # Construct the file path for the CSV file in each folder
    csv_file_path = os.path.join(cwd, folder, f'{folder}.csv')
    print(f"Processing {folder}")

    if os.path.isfile(csv_file_path):
        process_csv_file(csv_file_path, size_limit)
    else:
        print(f"No CSV file found for folder: {folder}")


# THE FOLLOWING CODE IS COMMENTED OUT AS IT IS ONLY NECESSARY WHEN DEALING WITH MORE THAN 64K ROWS OF DATA
# ===============================================================================
# ========== Fix the faulty entries in Chicago_Crimes_2001_to_2004.csv ==========
# ===============================================================================
# The faulty columns in row 1602849 are 'Y Coordinate', 'Year', 'Updated On'. But only 'Y Coordinate' column in the
# file contains None values. Thus, in row 1602849 we replace faulty 'Y Coordinate' by empty block, while 'Year' and
# 'Updated On' entries are taken from row 1602848

# file_path = cwd + '/Chicago_Crimes_2001_to_2004/Chicago_Crimes_2001_to_2004.csv'
# old_line = '3629582,1423259,G139165,03/10/2001 11:30:00 PM,035XX S FEDERAL ST,1340,CRIMINAL DAMAGE,TO STATE SUP PROP'\
#     ',CHA PARKING LOT/GROUNDS,True,False,211,2.0,,,14,1176246.0,18 08:55:02 AM,41.789832136,-87.672973835,"(41.78983'\
#     '2136, -87.672973835)"\n'
# new_line = '3629582,1423259,G139165,03/10/2001 11:30:00 PM,035XX S FEDERAL ST,1340,CRIMINAL DAMAGE,TO STATE SUP PROP'\
#     ',CHA PARKING LOT/GROUNDS,True,False,211,2.0,,,14,1176246.0,,2001,08/17/2015 03:03:40 PM,41.789832136,-87.672973'\
#     '835,"(41.789832136, -87.672973835)"\n'
#
## Read the file and replace the line
# with open(file_path, 'r') as file:
#    lines = file.readlines()
#
## Replace the line
# lines = [new_line if line == old_line else line for line in lines]
#
## Write the changes back to the file
# with open(file_path, 'w') as file:
#    file.writelines(lines)


# ===============================================================================
# ========== Fix the faulty entries in Chicago_Crimes_2005_to_2007.csv ==========
# ===============================================================================
# Here, in row 533719 we have a problem with 'Location' entry. But since location can be inferred by the 'Longitude'
# and 'Latitude' entries, we can fix this manually

# file_path = cwd + '/Chicago_Crimes_2005_to_2007/Chicago_Crimes_2005_to_2007.csv'
# old_line = '537288,5601758,HN409865,06/16/2007 08:15:00 PM,020XX E 94TH ST,1330,CRIMINAL TRESPASS,TO LAND,OTHER'\
#     'RAILROAD PROP / TRAIN DEPOT,False,False,413,4.0,8.0,48.0,26,1191237.0,1843038.0,2007,04/15/2016 08:55:02 A'\
#     'M,41.724300463,-87.575094193,"(41.724300463, -87.5,ID,Case Number,Date,Block,IUCR,Primary Type,Description,'\
#     'Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Ye'\
#     'ar,Updated On,Latitude,Longitude,Location\n'
# new_line = '537288,5601758,HN409865,06/16/2007 08:15:00 PM,020XX E 94TH ST,1330,CRIMINAL TRESPASS,TO LAND,OTHER '\
#     'RAILROAD PROP / TRAIN DEPOT,False,False,413,4.0,8.0,48.0,26,1191237.0,1843038.0,2007,04/15/2016 08:55:02 AM,'\
#     '41.724300463,-87.575094193,"(41.724300463, -87.575094193)"\n'
#
## Read the file and replace the line
# with open(file_path, 'r') as file:
#    lines = file.readlines()
#
## Replace the line
# lines = [new_line if line == old_line else line for line in lines]
#
## Write the changes back to the file
# with open(file_path, 'w') as file:
#    file.writelines(lines)


# ==================================
# ========== Clean az.csv ==========
# ==================================
# The “POSTCODE” column sometimes contains " ", which by our assumption denotes NaN. Thus, we replace " " by void so
# that the column type is BIGINT instead of VARCHAR.
# Moreover, row 2423531 contains "GLAMU" in “POSTCODE” row, which is not a valid postcode. Thus, we set that value to be
# void too.


# file_path = cwd + '/az/az.csv'
#
## Read the file and store lines in memory
# with open(file_path, 'r', encoding='utf-8') as file:
#    lines = file.readlines()
#
## Perform the replacements
# modified_lines = [line.replace(", ,", ",,") for line in lines]
# modified_lines = [line.replace(",GLAMU,", ",,") for line in modified_lines]
# modified_lines = [line.replace(",GARY,,a017b5cb9489f7de", ",,,a017b5cb9489f7de") for line in modified_lines]
# modified_lines = [line.replace(",GARY,,4510b55559cff995", ",,,4510b55559cff995") for line in modified_lines]
#
#
## Overwrite the original file with modified lines
# with open(file_path, 'w', encoding='utf-8') as file:
#    file.writelines(modified_lines)


# =============================================================
# ========== Change the delimiters in csv files to | ==========
# =============================================================

for index, row in df_filtered.iterrows():
    folder_name = row['filename'][:-4]
    original_delimiter = row['delimiter']
    file_path = os.path.join(os.getcwd(), folder_name, folder_name + '.csv')  # Construct file path

    if os.path.exists(file_path):
        # Read in the old file
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=original_delimiter if original_delimiter != '\\t' else '\t')
            rows = list(reader)

        # Write out the new file
        with open(file_path, mode='w', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='|')
            writer.writerows(rows)

        print(file_path, 'processed')
    else:
        print("Error with", file_path)

# Overwrite the original metadata file file with the filtered data
df_filtered.drop('delimiter', axis=1, inplace=True)
df_filtered.to_csv('metadata.csv', index=False)

folders = [f for f in os.listdir(cwd) if os.path.isdir(os.path.join(cwd, f))]


# =======================================================
# ========= Generate schema.yaml for all files ==========
# =======================================================
# When we generate the schema.yaml, we also get rid of the header of the .csv

# This is for correct .yaml formatting
class MyDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)


for folder in folders:
    csv_file_path = os.path.join(cwd, folder, f'{folder}.csv')

    if os.path.isfile(csv_file_path):
        con = duckdb.connect(database=':memory:')

        # DuckDB cannot correctly guess the formatting of some files. Thus, we sometimes help it 
        # The commented out lines may be useful when row number of files is > 64 * 1024
        if folder == 'wowah_data':
            con.execute(
                f"CREATE TABLE my_table AS SELECT * FROM read_csv('{csv_file_path}', "
                f"timestampformat='%m/%d/%y %H:%M:%S')")
        elif folder == 'us_perm_visas':
            con.execute(f"CREATE TABLE my_table AS SELECT * FROM read_csv('{csv_file_path}'," + """ types = {
            'employer_postal_code': 'VARCHAR'})""")
            # 'wage_offer_from_9089': 'VARCHAR',
            # 'wage_offer_to_9089': 'VARCHAR',
            # 'pw_amount_9089': 'VARCHAR'})""")
        # elif folder == 'business-licences':
        #    con.execute(f"CREATE TABLE my_table AS SELECT * FROM read_csv('{csv_file_path}'," + """ types = {
        #    'House': 'VARCHAR'})""")
        else:
            con.execute(f"CREATE TABLE my_table AS SELECT * FROM read_csv('{csv_file_path}')")

        # Retrieve and convert the schema to YAML
        schema_result = con.execute("DESCRIBE my_table").fetchall()
        # Transform the schema result into the desired format
        schema_formatted = {'columns': [{'name': col[0], 'type': col[1]} for col in schema_result]}
        schema_yaml = yaml.dump(schema_formatted, sort_keys=False)

        # Save the YAML
        yaml_file_path = os.path.join(cwd, folder, 'schema.yaml')
        with open(yaml_file_path, 'w') as f:
            yaml.dump(schema_formatted, f, sort_keys=False, Dumper=MyDumper, default_flow_style=False)

        # After saving the schema, remove the header from the CSV file
        # Read the CSV file again, this time as a list of lines
        with open(csv_file_path, 'r') as file:
            lines = file.readlines()

        # Write the lines back, excluding the first line (header)
        with open(csv_file_path, 'w') as file:
            file.writelines(lines[1:])

        print(f"Made schema.yaml and removed header for: {csv_file_path}")

        con.close()
    else:
        print(f"No CSV file found for folder: {folder}")