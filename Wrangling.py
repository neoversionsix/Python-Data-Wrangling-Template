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





# 2 READ MAIN DATA
#region
# Input Variables
#region
# Directory folder of the csv files you want to process
filename = 'C:\FILES\Hansen-Data-Qualified.csv'
# Can change to xlsx if needed, other changes will be nessesary to code
Extension = 'csv'
# Csv files seperator for input and output files..generally (,) or (|)
DeLimiter = ','
print('Directories loaded...')
#endregion
# Code
#region
#df_data = pd.read_csv(filename, sep=DeLimiter, engine='python', dtype=str, na_values=['',' '])
df_data = pd.read_csv(filename)
print(df_data.shape)
print(df_data.head())
print('Dataframe Loaded...')
#endregion
#endregion




# 3 VIEWING ISSUES WITH DATA
#region

# 3.1 Viewing a list of all the unique items in a column
# Switch WONO with name of the column
Unique_Array = df_data.WONO.unique()
Unique_Array.sort()
#export the Array
Output_Loc_Filname = r'C:\FILES\UniqueCodes2.csv'
pd.DataFrame(Unique_Array).to_csv(Output_Loc_Filname)

np.savetxt(Output_Loc_Filname , Unique_Array, delimiter=',', fmt='%s')

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

# 5.2 Delete Columns
#region
Cols_To_Delete = ['WQKey', 'WONO', 'ADDDTTM', 'SPOTVAL', 'COMMENTS', 'FLAG', 'ESTIMATED', 'FILENO', 'STATUS']
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

#5.5 Sorting the Table
#region
Columns_Sort_Order = ['WONO', 'SPOTCODE']
df_data.sort_values(by = Columns_Sort_Order, inplace=True)
print(df_data.head())
#endregion

#endregion





# 6 EXPORTING DATA
#region

# 6.1 Standard Export
#region
# Input Params
Output_Location = 'C:/FILES/'
Output_filename = 'Hansen-Data-Qualified3'
Output_Extension = '.csv'
Delimiter = ','

# Creating File
FName = Output_Location + Output_filename + Output_Extension
df_data.to_csv(path_or_buf=FName, sep= Delimiter, index=False)
print('---------------------------------------------')
print('DONE')
#endregion

# 6.2 Chuncked Export
#region
#INPUT PARAMS
Out_File_Loc_Name = 'C:/FILES/CHUNKS/CHUNK.csv'
Output_Extension = '.csv'
Delimiter = ','
ChunckSize = 500

#CODE
NumToRemove = -1 * (len(Output_Extension))
Output_filename = Out_File_Loc_Name[:NumToRemove]
Full_Filepath = Output_filename + Output_Extension
Row_Max = int((df_data.shape[0])-1)
NoChuncks = Row_Max/ChunckSize
NoChuncksInt = int(NoChuncks)
PartialChunck = NoChuncks - NoChuncksInt

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

print('EXPORTED CHUNKING DONE!!!!!!!!!!!!!!!!!!')
#endregion

#endregion