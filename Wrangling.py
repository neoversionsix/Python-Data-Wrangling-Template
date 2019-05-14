# 1 IMPORTING LIBRARIES -----------------------------------------------------------------------------------
#region
import pandas as pd
import os
import re
import glob
import csv
import shutil
import numpy as np
print('Libs Imported')
#endregion





# 2 READ DATA----------------------------------------------------------------------
#region
# Input Variables
#region
# Directory folder of the csv files you want to process
filename = r'C:\FILES\Hansen-Data-Qualified8.csv'
print('Data will load from: ', filename)
# Can change to xlsx if needed, other changes will be nessesary to code
Extension = 'csv'
# Csv files seperator for input and output files..generally (,) or (|)
DeLimiter = ','
print('Directories loaded.')
print('Loading Data...')
#endregion
# Code
#region
df_data = pd.read_csv(filename, sep=DeLimiter, engine='python', dtype=str)
print('-------------SHAPE--------------')
print(df_data.shape)
print('----------DATAFRAME-------------')
print(df_data.head(1))
print('--------------------------------')
print('Dataframe Loaded.')
#endregion
#endregion





# 3 VIEWING ITEMS IN COLUMNS -----------------------------------------------------------
#region
# 3.1 Viewing a list of all the unique items in a column
#region
# Input Params
Column_Name_To_Check3 = 'SPOTCODE'
# Create Array of Unique Items
# Swap the name of the column to rename
df_data.rename(columns={Column_Name_To_Check3: 'coltocheck'}, inplace=True)
# Check Data
Unique_Array = df_data.coltocheck.unique()
Unique_Array.sort()
# Swap back the name of the column to rename
df_data.rename(columns={'coltocheck': Column_Name_To_Check3}, inplace=True)
#export the Unique Items Array
Output_Loc_Filname = r'C:\FILES\UniqueCodes2.csv'
pd.DataFrame(Unique_Array).to_csv(Output_Loc_Filname)
np.savetxt(Output_Loc_Filname , Unique_Array, delimiter=',', fmt='%s')
#End-3.1
#endregion
# 3.2 Comparting Rows in a Column with items in a lookup table
#region
Data_Column_Name= 'SPOTCODE-E'
Create_Unmapped_CSV = 'y'
Lookup_Table_Dir = r'C:\FILES\Hansen-Methods-In-EnviroSys.xlsx'
Sheet_To_Load = 'Data'
Lookup_Column_Name_To_Check = 'Method Short Name'
Output_Loc_Filname = r'C:\FILES\UniqueCodesUnmapped5.csv'
#Load Lookup Table
df_lookup = pd.read_excel(Lookup_Table_Dir ,
    sheet_name = Sheet_To_Load,
    dtype=object)
print('loaded lookup table.')
print('Rows, Columns:')
print(df_lookup.shape)
# Delete All columns in lookup table except
Cols_Dont_Delete = [Lookup_Column_Name_To_Check]
df_lookup.drop(df_lookup.columns.difference(Cols_Dont_Delete), 1, inplace=True)
print('Columns Deleted.')
print('Rows, Columns:')
print(df_lookup.shape)
print('Generating Bools Filter...')
Bools_Mapping_Series = df_data[Data_Column_Name].isin(df_lookup[Lookup_Column_Name_To_Check])
print('Bools Generated.')
print('Creating dataframe with filtered data df_data2...')
df_data2 = df_data[Bools_Mapping_Series]
df_deleted = df_data[~Bools_Mapping_Series]
print('Data Filtered and now in df_data2')
df_deleted.rename(columns={Data_Column_Name: 'coltocheck'}, inplace=True)
Unique_Array_Unmapped = df_deleted.coltocheck.unique()
Unique_Array_Unmapped.sort()
print('Created ndarray of unmapped items called: Unique_Array_Unmapped')
df_data.rename(columns={'coltocheck': Data_Column_Name}, inplace=True)
#export the Unique Items Array to a CSV
if Create_Unmapped_CSV == 'y':
    print('Exporting Unmapped Items to a CSV...')
    pd.DataFrame(Unique_Array_Unmapped).to_csv(Output_Loc_Filname)
    np.savetxt(Output_Loc_Filname , Unique_Array_Unmapped, delimiter=',', fmt='%s')
    print('Unique Unmapped CSV created.')
    print('See Location: ', Output_Loc_Filname)
    print('DONE!')
    print('------------------------------------')
#End-3.2
#endregion
#End-3
#endregion





# 5 DATA WRANGLING
#region

# 5.1 Deleting Rows
#region
# 5.1.1 Deleting Rows with everything missing in the row
#region
# Checking number of rows
rows_count = df_data.shape[0]
# Dropping rows
df_data2 = df_data.dropna(axis='index', how='all')
df_data = df_data2
# Checking number of rows removed
rows_deleted = df_data.shape[0] - rows_count
# Printing rows removed
print("Number of Rows Deleted =", rows_deleted)
#endregion
# 5.1.2 Deleting Rows with missing info in certain columns
#region
# Input Parameters
Cols_To_Check = ['WONO', 'SPOTVAL', 'ADDDTTM']
#note 'any' will delete a row if any of the columns is mssing data
#note 'all' will only deleter a row if all the columns above are NA.
How_to_delete = 'any'
# Checking number of rows
rows_count = df_data.shape[0]
# Dropping rows
df_data2 = df_data.dropna(axis='index', how = How_to_delete, subset = Cols_To_Check)
df_data = df_data2
df_var_checker = df_data.head(5)
# Checking number of rows removed
rows_deleted = df_data.shape[0] - rows_count
# Printing rows removed
print("Number of Rows Deleted =", rows_deleted)
#endregion
# 5.1.3 Deleting Rows matching a certain string in a certain column
#region
Column_Name_To_Check = "SPOTVAL"
String_To_Check = ' '
# Bools_With_String = df_data[Column_Name_To_Check].str.contains(String_To_Check)
# Bools_With_String = df_data[Column_Name_To_Check].str.startswith(String_To_Check)
Bools_With_String = df_data[Column_Name_To_Check].str.match(String_To_Check)
Bools_Without_String = ~Bools_With_String
df_data2 = df_data[Bools_Without_String]
Row_Difference = df_data.shape[0] - df_data2.shape[0]
df_data = df_data2
print('Rows Deleted: ', Row_Difference)


'''Working progress...
Strings_To_Check = [' ', '  ']
Columns_To_Check = ['WONO']
df_data2 = df_data.filter(axis='index', items = Columns_To_Check, regex=' ')
'''

#endregion
# 5.1.4 Deleting and/or Checking Rows that don't map to items in a lookup table
#region
Delete_Unmapped = 'n'
Create_Unmapped_CSV = 'y'
Lookup_Table_Dir = r'C:\FILES\Hansen-Methods-In-EnviroSys.xlsx'
Output_Loc_Filname = r'C:\FILES\UniqueCodesUnmapped5.csv'
Sheet_To_Load = 'Data'
Lookup_Column_Name_To_Check = 'Method Short Name'
Column_Name_To_Apply_Deletions = 'SPOTCODE-E'
#Load Lookup Table
df_lookup = pd.read_excel(Lookup_Table_Dir ,
    sheet_name = Sheet_To_Load,
    dtype=object)
print('loaded lookup table.')
print('Rows, Columns:')
print(df_lookup.shape)
# Delete All columns except
Cols_Dont_Delete = [Lookup_Column_Name_To_Check]
df_lookup.drop(df_lookup.columns.difference(Cols_Dont_Delete), 1, inplace=True)
print('Columns Deleted.')
print('Rows, Columns:')
print(df_lookup.shape)
print('Generating Bools Filter...')
Bools_Mapping_Series = df_data[Column_Name_To_Apply_Deletions].isin(df_lookup[Lookup_Column_Name_To_Check])
print('Bools Generated.')
df_data2 = df_data[Bools_Mapping_Series]
df_deleted = df_data[~Bools_Mapping_Series]
print('Data Filtered and now in df_data2')
df_deleted.rename(columns={Column_Name_To_Apply_Deletions: 'coltocheck'}, inplace=True)
Unique_Array_Unmapped = df_deleted.coltocheck.unique()
Unique_Array_Unmapped.sort()
print('Created ndarray of unmapped items called: Unique_Array_Unmapped')
df_data.rename(columns={'coltocheck': Column_Name_To_Apply_Deletions}, inplace=True)
#export the Unique Items Array to a CSV
if Create_Unmapped_CSV == 'y':
    print('Exporting Unmapped Items to a CSV...')
    pd.DataFrame(Unique_Array_Unmapped).to_csv(Output_Loc_Filname)
    np.savetxt(Output_Loc_Filname , Unique_Array_Unmapped, delimiter=',', fmt='%s')
    print('Unique Unmapped CSV created.')
    print('See Location: ', Output_Loc_Filname)
# Effectively Deleting Unmapped Items if requested
if Delete_Unmapped == 'y':
    print('deleting Unmapped Items from df_data')
    df_data = df_data2
else:
    print('Original Data is untouched in df_data and filtered data is stored in df_data2')
#End 5.1.4
#endregion

#End 5.1
#endregion

# 5.2 Delete Columns
#region
Cols_To_Delete = [
    'WONO',
    'SPOTCODE',
    ]
df_data.drop(Cols_To_Delete, axis=1, inplace=True)
df_data.head()
#endregion

# 5.3 Concatenate Columns to Create Keys
#region
Name_of_New_Col = 'KEY'
Cols_To_Join = ['SPOTCODE', 'UM', 'DESCRIPT']
df_data = df_data.astype(str)
df_data[Name_of_New_Col] = df_data[Cols_To_Join].apply(lambda x: '-'.join(x.map(str)), axis=1)
df_data.head()
#endregion

# 5.4 Swapping Data in a Column based on a csv lookup table
#region
# Input Params
#region
# Column to apply the Swapping to
ColName = 'WONO'
# Directory file of Lookup Table for swapping
# Note: have to columns named 'FIND' and 'REPLACE'
Input_path_Lookup = 'C:/FILES/find-replace.csv'
#endregion

# Read and process the lookup table
#region
df_lookup = pd.read_csv(Input_path_Lookup, dtype={'FIND': object, 'REPLACE': object})
# Delete Rows with everything missing in the row
df_lookup = df_lookup.dropna(axis='index', how='all')
# Delete non Unique (duplicate) FIND rows
df_lookup.drop_duplicates(subset='FIND', keep=False, inplace=True)
# Create a list of Unique Find items
List_Subs = df_lookup['FIND'].tolist()
# Change index to FIND
df_lookup.set_index('FIND', inplace = True)
dict_Subs = df_lookup.to_dict()
dict_Subs = dict_Subs.get('REPLACE')
print('SUBSTITUTIONS...')
print(dict_Subs)
#endregion

# Apply swap changes to dataframe
#region
#    Swap the name of the column to rename
df_data.rename(columns={ColName: 'coltoswapxy'}, inplace=True)
#    Make the replacements
df_data2 = df_data.coltoswapxy.replace(dict_Subs)
#    Swap back the name of the column to rename
df_data2.rename(columns={'coltoswapxy': ColName}, inplace=True)
df_data = df_data2
print(df_data.head())
print('DONE SWAPPING')
#endregion
#endregion

# 5.5 Sorting the Table
#region
Columns_Sort_Order = ['WONO-E', 'SPOTCODE-E']
df_data.sort_values(by = Columns_Sort_Order, inplace=True)
print(df_data.head())
#endregion

# 5.6 Editing Data
#region
# 5.6.1 Delete spaces from start and end of a cell swap to 'lstrip' or 'rstrip' if required
#region
Column_To_Edit = 'SPOTCODE'
Delete_Original_Column = 'n'
Edited_Col_Name = Column_To_Edit + '-E'
df_data.rename(columns={Column_To_Edit: 'editingthiscolumn'}, inplace=True)
New_Col_Edited = df_data.editingthiscolumn.str.strip()
df_data[Edited_Col_Name] = New_Col_Edited
df_data.rename(columns={'editingthiscolumn': Column_To_Edit}, inplace=True)
if Delete_Original_Column == 'y':
    df_data.drop(Column_To_Edit, axis=1, inplace=True)
    df_data.rename(columns={Edited_Col_Name: Column_To_Edit}, inplace=True)
#End-5.6.1
#endregion

# 5.6.2 Delete a character from the end of a cell
#region
Column_To_Edit = 'WONO-E'
Delete_Original_Column = 'y'
String_To_Strip = '/'
Edited_Col_Name = Column_To_Edit + '-E'
df_data.rename(columns={Column_To_Edit: 'editingthiscolumn'}, inplace=True)
New_Col_Edited = df_data.editingthiscolumn.str.rstrip(String_To_Strip)
df_data[Edited_Col_Name] = New_Col_Edited
df_data.rename(columns={'editingthiscolumn': Column_To_Edit}, inplace=True)
if Delete_Original_Column == 'y':
    df_data.drop(Column_To_Edit, axis=1, inplace=True)
    df_data.rename(columns={Edited_Col_Name: Column_To_Edit}, inplace=True)
#endregion
#End-5.6.2

# 5.6.5 Adding a string to the start of a row based on condition
#region
String_To_Check = 'SPT0'
Column_Name_To_Check = 'WONO-E'
String_To_Add = 'HANSEN-'

# Get Indexes of Rows
df_data.reset_index(drop=True)
Bools_With_String = df_data[Column_Name_To_Check].str.startswith(String_To_Check)
Index_Array = Bools_With_String[Bools_With_String].index.values

# Edit Cells
for item in Index_Array:
    x = df_data.at[item, Column_Name_To_Check]
    new_string = String_To_Add + str(x)
    df_data.at[item, Column_Name_To_Check] = new_string

#End 5.6.5
#endregion

#End-5.6
#endregion

#End-5
#endregion





# 6 EXPORTING DATA
#region

# 6.1 Standard Export
#region
# Input Params
Output_Location = 'C:/FILES/'
Output_filename = 'Hansen-Data-Qualified8'
Output_Extension = '.csv'
Delimiter = ','

# Creating File
FName = Output_Location + Output_filename + Output_Extension
df_data.to_csv(path_or_buf=FName, sep= Delimiter, index=False)
print('---------------------------------------------')
print('EXPORTING CSV DONE')
#endregion

# 6.2 Chuncked Export
#region
#INPUT PARAMS
Out_File_Loc_Name = 'C:/FILES/HCHUNKS/HANSENDB_.csv'
Output_Extension = '.csv'
Delimiter = '|'
ChunckSize = 1000

#CODE
NumToRemove = -1 * (len(Output_Extension))
Output_filename = Out_File_Loc_Name[:NumToRemove]
Full_Filepath = Output_filename + Output_Extension
Row_Max = int((df_data.shape[0])-1)
NoChuncks = Row_Max/ChunckSize
NoChuncksInt = int(NoChuncks)
PartialChunck = NoChuncks - NoChuncksInt

print('Chunking Data...')
if PartialChunck > 0:
    Bool_PartialChunck = True
else:
    Bool_PartialChunck = False

counter = int(0)
FromRow = 0
ToRow = ChunckSize

while counter <= NoChuncksInt:
    df_temp=df_data.iloc[FromRow:ToRow, :]
    FromRow = ToRow
    ToRow = ToRow + ChunckSize
    counter += 1
    FName = Output_filename + str(counter) + Output_Extension
    df_temp.to_csv(path_or_buf=FName, sep=Delimiter, index=False)

if PartialChunck == True:
    counter += 1
    FName = Output_filename + str(counter) + Output_Extension
    df_temp=df_data.iloc[FromRow::, :]
    df_temp.to_csv(path_or_buf=FName, sep=Delimiter, index=False)

print('EXPORTED CHUNKING DONE!')
#endregion

#endregion