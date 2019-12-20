# Imports
import pandas as pd
import pymysql
import cryptography

# Read Data
ipps_df = pd.read_csv('ipps.csv')

# Rename Columns to something reasonable to work with
ipps_df.columns = ['drg',
                   'prov_ID',
                   'prov_name',
                   'prov_address',
                   'prov_city',
                   'prov_state',
                   'prov_zip',
                   'hrr',
                   'discharge_count',
                   'covered_charges',
                   'total_payments',
                   'medicare_payments']

# Add an hrr_ID
ipps_df['hrr_ID'] = ipps_df.groupby('hrr').ngroup()

# Create a new dataframe for hrr
hrr_df = pd.DataFrame({'hrr': list(ipps_df.hrr), 'hrr_ID': list(ipps_df.hrr_ID)})

# Split the columns, drop the original column
hrr_df[['hrr_state', 'hrr_city']] = hrr_df['hrr'].str.split(' - ',expand=True)
hrr_df = hrr_df[['hrr_ID', 'hrr_city', 'hrr_state']]

# Drop duplicates
hrr_df = hrr_df.drop_duplicates()

# Split the column in the original datafram
ipps_df[['hrr_state', 'hrr_city']] = ipps_df['hrr'].str.split(' - ', expand=True)

# Drop hrr, hrr_state
ipps_df = ipps_df.drop(['hrr', 'hrr_state'], axis=1)

# Create new dataframe for drg, # Drop dupes
drg_df = pd.DataFrame({'drg': list(ipps_df.drg)})
drg_df = drg_df.drop_duplicates()

# Split the columns, drop the original column
drg_df[['drg_ID', 'drg_desc']] = drg_df['drg'].str.split(' - ',expand=True)
drg_df = drg_df.drop('drg', axis=1)

# Split the columns in the original dataframe, and drop extraneous
ipps_df[['drg_ID', 'drg_desc']] = ipps_df['drg'].str.split(' - ', expand=True)
ipps_df = ipps_df.drop(['drg_desc', 'drg'], axis=1)

# Create the dataframe, drop dupes
prov_df = ipps_df[['prov_ID',
                   'prov_name',
                   'prov_address',
                   'prov_city',
                   'prov_state',
                   'prov_zip', 'hrr_ID']]

prov_df = prov_df.drop_duplicates()

## Drop the columns from original dataframe
ipps_df = ipps_df.drop(['prov_name',
                        'prov_address',
                        'prov_state',
                        'prov_city',
                        'prov_zip',
                        'hrr_ID'], axis=1)

# Reorganize what's left for the payments relation
payments_df = ipps_df
payments_df['payment_ID'] = range(len(payments_df))

payments_df = payments_df[['payment_ID',
                           'prov_ID',
                           'drg_ID',
                           'discharge_count',
                           'covered_charges',
                           'total_payments',
                           'medicare_payments']]

# Connect to DB
server = 'localhost'
database = 'ipps'
user = 'ipps'
password = 'password'

connection = pymysql.connect(host = server, user = user, password = password, db = database)

# Populate new rows for HRR Relation
cursor = connection.cursor()
sql = 'INSERT INTO HRR VALUES (%s, %s, %s)'

for row in range(len(hrr_df)):
    cursor.execute(sql, tuple([float(hrr_df.iloc[row,0]),
                        str(hrr_df.iloc[row,1]),
                        str(hrr_df.iloc[row,2])]))

connection.commit()

# Populate new rows for DRG Relation
sql = 'INSERT INTO DRG VALUES (%s, %s)'

for row in range(len(drg_df)):
    cursor.execute(sql, tuple([float(drg_df.iloc[row,0]),
                               str(drg_df.iloc[row,1])]))

connection.commit()

# Populate new rows for Providers Relation
cursor = connection.cursor()
sql = 'INSERT INTO Providers VALUES (%s, %s, %s, %s, %s, %s, %s)'

for row in range(len(prov_df)):
    cursor.execute(sql, tuple([int(prov_df.iloc[row,0]),
                               prov_df.iloc[row,1],
                               prov_df.iloc[row,2],
                               prov_df.iloc[row,3],
                               prov_df.iloc[row,4],
                               int(prov_df.iloc[row,5]),
                               int(prov_df.iloc[row,6])]))

connection.commit()

# Populate new rows for Payments Relation
sql = 'INSERT INTO Payments VALUES (%s, %s, %s, %s, %s, %s, %s)'

for row in range(len(payments_df)):
    cursor.execute(sql, tuple([int(payments_df.iloc[row,0]),
                               int(payments_df.iloc[row,1]),
                               int(payments_df.iloc[row,2]),
                               int(payments_df.iloc[row,3]),
                               float(payments_df.iloc[row,4]),
                               float(payments_df.iloc[row,5]),
                               float(payments_df.iloc[row,6])]))

connection.commit()

# Close connection to DB
connection.close()
