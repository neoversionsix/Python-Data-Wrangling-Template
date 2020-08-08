# 1 IMPORTING LIBRARIES -------------------------------------------------------------------
#region
import pandas as pd
import numpy as np
import os
import re
import glob
import csv
import shutil
print('Libraries Imported.')
#endregion





# 2 READ DATA----------------------------------------------------------------------
#region
# 2.1 Read a Single CSV file
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
# Code
df_data = pd.read_csv(filename, sep=DeLimiter, engine='python', dtype=str)
print('-------------SHAPE--------------')
print(df_data.shape)
print('----------DATAFRAME-------------')
print(df_data.head(1))
print('--------------------------------')
print('Dataframe Loaded.')
#End 2.1
#endregion

# 2.2 Read a Single XLSX file
#region
#End 2.2
#endregion

#End 2
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





# 7 Bulk edit csv files

# 7.1 Full MW cleanse and move
#region
# INPUT VARIABLES----------------------------------------------------------------------------------------
#region
# Directory folder of the csv files you want to process
Input_path_CSVs = 'C:/FILES/Input_CSV/'
# Can change to xlsx if needed, other changes will be nessesary to code
Extension = 'csv'
# Csv files seperator for input and output files..generally (,) or (|)
Delimiter = '|'

# Directory folder of the TKC cross reference table
Input_path_TKC_files = 'D:/FILES/Input_TKC_files/'

# Directory excel file of the Sample Point Table
Input_path_SPT = 'C:/FILES/Sample Points_37883_20180926_134607.xlsx'

# Output folder of the CSV files
Output_path_processed_csv = 'C:/FILES/Output_CSV_Processed/'

# Output folder path of bad SPT CSV files
Output_path_badSPT = 'C:/FILES/Output_CSV_Bad_SPT/'

# Output folder path of TKC Unmapped Data
Output_path_badTKC = 'C:/FILES/Output_CSV_Bad_TKC/'

# Output folder path of Retest CSV files
Output_path_Retests = 'C:/FILES/Output_CSV_Retests/'

# Output folder path of CSV Files with Structure that can't be Analysed
Output_path_bad_structure = 'C:/FILES/Output_CSV_Bad_Column_Structure/'

# Output folder path of Report on Analysed files
Output_path_Report = 'C:/FILES/'

print('Directories loaded...')
#endregion

# READ AND PROCESS THE UNIQUE SAMPLE POINTS FILE----------------------------------------------------------------
#region
df_SPTs = pd.read_excel(Input_path_SPT, sheet_name='Data', dtype={'Name': object, 'OldSiteCode_post2007': object})
List_Columns_Keep = ['Name','OldSiteCode_post2007']
df_SPTs = df_SPTs[List_Columns_Keep]
df_SPTs.columns = ['SPT', 'OSC']

# Remove ESP points
df_SPTs = df_SPTs[~df_SPTs['SPT'].astype(str).str.startswith('ESP')]

# Delete non Unique (duplicate) OSC's
df_SPTs.drop_duplicates(subset='OSC', keep=False, inplace=True)

# Create a list of Unique OSC's
List_OSCs = df_SPTs['OSC'].tolist()

# Change index to old site code
df_SPTs.set_index('OSC', inplace = True)

dict_SPTs = df_SPTs.to_dict()
dict_SPTs = dict_SPTs.get('SPT')

#print('SPT Cross Reference Table created...')
#endregion

# READ AND PROCESS THE TKC FILES-------------------------------------------------------------------------
#region
os.chdir(Input_path_TKC_files)
filenames = [i for i in glob.glob('*.{}'.format('xlsx'))]

List_Columns_Keep = ['Test Key Code (TKC)','Valid', 'Data Type']
bool_df_created = False
for filename in filenames:
    if bool_df_created == False:
        df_TKCs = pd.read_excel(filename, sheet_name='Data', dtype={'Test Key Code (TKC)': object})
        bool_df_created = True
        df_TKCs = df_TKCs[List_Columns_Keep]
        df_TKCs.columns = ['TKC', 'Valid','DT']
    else:
        df_temp = pd.read_excel(filename, sheet_name='Data', dtype={'Test Key Code (TKC)': object})
        df_temp = df_temp[List_Columns_Keep]
        df_temp.columns = ['TKC', 'Valid','DT']
        df_TKCs = df_TKCs.append(df_temp)

# Remove Biosolids Monitoring TKC's from Dataframe and delete DT column
df_TKCs = df_TKCs[~df_TKCs['DT'].astype(str).str.startswith('Bio')]
df_TKCs.drop('DT', axis=1, inplace=True)

# Remove Invalid TKC's from Dataframe and delete Valid column
df_TKCs = df_TKCs[~df_TKCs['Valid'].astype(str).str.startswith('N')]
df_TKCs.drop('Valid', axis=1, inplace=True)

# Delete non Unique (duplicate) TKC's
df_TKCs.drop_duplicates(subset='TKC', keep=False, inplace=True)

#Create List of Mapped TKCs
List_TKCs = df_TKCs['TKC'].tolist()

print('Mapped TKC List created...')

#endregion

# SAVE CSV FILENAMES IN A LIST AND DATAFRAME-------------------------------------------------------------
#region
# Get the csv filenames into an array
os.chdir(Input_path_CSVs)
filenames = [i for i in glob.glob('*.{}'.format(Extension))]

# Get the number of csv files
NumFiles = len(filenames)
print(NumFiles, 'csv files found...')
#endregion

# MOVE FILES WITHOUT 'LOCATIONCODE' or 'LocationDescription' --------------------------------------------
#region
counter_good_files = 0
counter_bad_files = 0
List_Unsupported_Files = []
for filename in filenames:
    # Save an individual file as a DataFrame Object to analyse
    try:
        df_file = pd.read_csv(filename, sep=Delimiter, index_col=False, engine='python')
        if ('LOCATIONCODE' in df_file.columns) and ('LocationDescription' in df_file.columns):
            counter_good_files +=1
        else:
            List_Unsupported_Files.append(filename)
            counter_bad_files +=1
    except:
        List_Unsupported_Files.append(filename)
        counter_bad_files +=1

# Print stats        
print('Number of Files that can be Analysed:', counter_good_files)
print("Number of Files that can't be Analysed:", counter_bad_files)

# Move files
files = os.listdir(Input_path_CSVs)

for f in files:
    if f in List_Unsupported_Files:
        shutil.move(f, Output_path_bad_structure)


# Get the csv filenames into an array after unwanted ones are moved
os.chdir(Input_path_CSVs)
filenames = [i for i in glob.glob('*.{}'.format(Extension))]

#endregion

# CREATE AN EMPTY DATAFRAME FOR REPORT-------------------------------------------------------------------
#region
List_Columns = ['Filename', 'Total Rows', 'Duplicates', 'Retests', 'Rows QA Data', 'No SPT Code', 'Replaced SPT Codes', 'Rows With Unmapped TKCs']
df_Report = pd.DataFrame(columns=List_Columns)
#endregion

# LOOP THOUGH EACH FILE AND PROCESS IT ------------------------------------------------------------------

for filename in filenames:
    print('-----------------------------------------------')
    print('current file:')
    print(filename)

    # Set Booleans
    QA_Data_In_File = False
    Bad_sptz = False
    Retests_In_File = False
    Duplicates_In_File = False
    Bad_TKCs = False
    
    # Set Counts
    Int_Total_Rows = int(0)
    Int_Bad_SPTs = int(0)
    Int_Replaced_SPTs = int(0)
    Int_QA_Rows = int(0)
    Int_Dup_Rows = int(0)
    Int_Retest_Rows = int(0)
    Int_Unmapped_TKCs = int(0)

    # Save the individual file as a DataFrame Object to analyse
    df_file = pd.read_csv(filename, sep=Delimiter, index_col=False, engine='python', dtype={'LOCATIONCODE': object, 'TEST_KEY_CODE': object})
    # Delete Rows with everything missing in the row
    df_file = df_file.dropna(axis='index', how='all')
    Int_Total_Rows = df_file.shape[0]

    ###################  QA DATA DELETION  ##################################
    # Check and Find if Blanks exist in Location Code Rows
    bools_ml = df_file['LOCATIONCODE'].isnull()
    bools_ml = np.array(bools_ml)
    
    # Check and Find if/Where Blanks exist in Location Description Rows
    bools_md = df_file['LocationDescription'].isnull()
    bools_md = np.array(bools_ml)

    # Find Quality Assurance Data Rows
    Series_Loc_Desc = df_file['LocationDescription']
    Series_Loc_Desc = Series_Loc_Desc.fillna(' ')
    bools_bd = Series_Loc_Desc == 'Blind Dup A'
    bools_bd = np.array(bools_bd)
    bools_bd = np.logical_and(bools_ml, bools_bd)
    bools_fb = Series_Loc_Desc == 'Field Blank'
    bools_fb = np.array(bools_fb)
    bools_fb = np.logical_and(bools_ml, bools_fb)
    bools_qa = np.logical_or(bools_bd, bools_fb)    
    bools_filter_QA = np.invert(bools_qa)
    if False in bools_filter_QA:
        QA_Data_In_File = True
        
    # Remove QA Data and update DataFrame object if neccesary
    if QA_Data_In_File == True:
        # Number of QA Rows in data
        Int_QA_Rows = np.sum(bools_qa)
        # Filter Out Quality Control Data from DataFrame Object
        df_file = df_file[bools_filter_QA]

    ################### SWAP OLD CODES FOR SPT CODES ####################
        # Count the number of replacements that can be made
    for item in df_file['LOCATIONCODE']:
        if item in List_OSCs:
            Int_Replaced_SPTs +=1

    # Make the replacements
    df_file.LOCATIONCODE.replace(dict_SPTs , inplace = True)
       
    ###################  MISSING SPT MOVE  ############################
    # Check if SPT's still don't exist in Location Code Rows
    Array_Loc_Codes = df_file['LOCATIONCODE']
    # Create Array of Loc-Codes and Fill Blank Locations with a space so that we can check if it begins with SPT
    Array_Loc_Codes = Array_Loc_Codes.fillna(' ')
    # Generate Array of Booleans for rows that have SPT's
    bools_good_sptz = Array_Loc_Codes.str.startswith('SPT')
    bools_good_sptz = np.array(bools_good_sptz)

    # Save bad SPT codes in data to a new dataframe
    if False in bools_good_sptz:
        Bad_sptz = True
        # Create dataframe for stuff that can't load
        df_cant_load = df_file
        bools_filter_badSPT = np.invert(bools_good_sptz)
        # Count the number of Bad SPTz
        Int_Bad_SPTs = np.sum(bools_filter_badSPT)
        # Leave only Bad SPT data in 'df_cant_load' Dataframe
        df_cant_load = df_cant_load[bools_filter_badSPT]        
        # Filter Orginal Dataframe 'df_file' to delete SPT rows that won't load
        df_file = df_file[bools_good_sptz]

    ###################  UNMAPPED TKC MOVE  ############################
    
    Array_TKC_Codes = df_file['TEST_KEY_CODE']
    Array_TKC_Codes = Array_TKC_Codes.fillna(' ')
    Lst_files_TKCs = Array_TKC_Codes.tolist()
    
    # Generate Array of Booleans for rows that have mapped TKC's
    Lst_bools_good_TKCs = []
    for item in Lst_files_TKCs:
        if item in List_TKCs:
            Lst_bools_good_TKCs.append(True)
        else:
            Lst_bools_good_TKCs.append(False)

    bools_good_TKCs = np.array(Lst_bools_good_TKCs)

    # Save bad TKC codes in data to a new dataframe
    if False in bools_good_TKCs:
        Bad_TKCs = True
        Int_Unmapped_TKCs = Lst_bools_good_TKCs.count(False)
        # Create dataframe for stuff that can't load
        df_cant_load_TKCs = df_file
        bools_filter_badTKC = np.invert(bools_good_TKCs)
        # Count the number of Bad SPTz
        Int_Bad_TKCs = np.sum(bools_filter_badTKC)
        # Leave only Bad SPT data in 'df_cant_load_TKCs' Dataframe
        df_cant_load_TKCs = df_cant_load_TKCs[bools_filter_badTKC]        
        # Filter Orginal Dataframe 'df_file' to delete TKC rows that won't load
        df_file = df_file[bools_good_TKCs]
    
    ###################  EXACT DUPLICATE DELETION  ####################################
    # Create Lists to Check for duplicates
    List_Row_Key = []
    for index, row in df_file.iterrows():
        String_Row =  str(row['LOCATIONCODE']) + str(row['SAMPLEDATE']) + str(row['TEST_KEY_CODE']) + str(row['RESULT'])
        # Append the row to the list of Rows to Check for Duplicates
        List_Row_Key.append(String_Row)

    # Create Array of Rows from List of Rows
    Array_Raw_Items_Results = np.array(List_Row_Key)
    
    # Decide if there are any exact duplicates    
    if len(np.unique(Array_Raw_Items_Results)) != len(Array_Raw_Items_Results):
        Duplicates_In_File = True
    
    # Find the rows where the exact duplicates live
    if Duplicates_In_File == True:
        # Create a dictionary of duplicate checker keys vs numvber of duplicates
        unique, counts = np.unique(Array_Raw_Items_Results, return_counts=True)
        Dict_Unique_Counts = dict(zip(unique, counts))
        #Create a list of keys for duplicates only
        List_Duplicate_Keys = ([key for key, val in Dict_Unique_Counts.items() if val > 1])
        # Find which Row Strings are exact duplicates and add their Index to a list
        List_Row_Index_Duplicates = []
        for key in List_Duplicate_Keys:
            # Get the row index's of the duplicate Key
            List_Indexes = [i for i, j in enumerate(List_Row_Key) if j == key]
            # Remove Fist Duplicate from List because we let the first one load
            List_Indexes.pop(0)
            # Append the row index's to a list
            List_Row_Index_Duplicates.extend(List_Indexes)
        # Count the number of Duplicates that will be deleted for Report
        Int_Dup_Rows = len(List_Row_Index_Duplicates)
        # Print feedback on Rows that are being deleted
        print('Deleting Duplicate Rows from:', filename)
        print(List_Row_Index_Duplicates)
        # Delete the rows that are duplicates from dataframe df_file
        df_file = df_file.drop(df_file.index[List_Row_Index_Duplicates])
        
    ###################  RETESTS MOVE ############################
    # Create Lists to Check for Retests
    List_Row_Key = []
    for index, row in df_file.iterrows():
        String_Row =  str(row['LOCATIONCODE']) + str(row['SAMPLEDATE']) + str(row['TEST_KEY_CODE'])
        # Append the Row String Item to the list of Rows
        List_Row_Key.append(String_Row)

    # Create Array of Rows from List of Rows
    Array_Raw_Items_Results = np.array(List_Row_Key)
    # Decide if there are any Retests   
    if len(np.unique(Array_Raw_Items_Results)) != len(Array_Raw_Items_Results):
        Retests_In_File = True
    
    # Find the rows where the Retests live
    if Retests_In_File == True:
        # Create a dictionary of Row Strings to number of Retests
        unique, counts = np.unique(Array_Raw_Items_Results, return_counts=True)
        Dict_Unique_Counts = dict(zip(unique, counts))
        #Create a list of keys for Retests only
        List_Retest_Keys = ([key for key, val in Dict_Unique_Counts.items() if val > 1])
        # Find which Row Indexes are exact duplicates and add their Index to a list
        List_Row_Index_Retests = []
        for key in List_Retest_Keys:
            # Get the row index's of the duplicate Key
            List_Indexes = [i for i, j in enumerate(List_Row_Key) if j == key]
            # Remove Fist Duplicate from List because we let the first one load
            List_Indexes.pop(0)
            # Append the row index's to a list
            List_Row_Index_Retests.extend(List_Indexes)
        # Count the number of Retests
        Int_Retest_Rows = len(List_Row_Index_Retests)
        # Print feedback on Rows that are being deleted
        print('Moving Retests from', filename)
        print(List_Row_Index_Retests)
        
        # Generate bools for Retest Rows
        Num_New_Rows = df_file.shape[0]
        List_Bools_Retests = []
        Counter = int(0)
        for i in range(Num_New_Rows):
            if Counter in List_Row_Index_Retests:
                List_Bools_Retests.append(True)
                Counter += 1
            else:
                List_Bools_Retests.append(False)
                Counter += 1
        
        # Convert the list of Bools to a Numpy Array
        Array_Bools_Retests = np.array(List_Bools_Retests)
        
        # Create a new dataframe 'df_Retests' for only Retests
        df_Retests = df_file[Array_Bools_Retests]
                
        # Delete the rows that are Retests from dataframe df_file
        df_file = df_file.drop(df_file.index[List_Row_Index_Retests])
        
        
    ###################  REPORT UPDATING ############################
    # Append Update the Report Dataframe
    List_Row_Report = [filename, Int_Total_Rows, Int_Dup_Rows, Int_Retest_Rows, Int_QA_Rows, Int_Bad_SPTs, Int_Replaced_SPTs, Int_Unmapped_TKCs]
    df_Temp_Report = pd.DataFrame([List_Row_Report], columns=List_Columns)
    df_Report = df_Report.append(df_Temp_Report, ignore_index=True)
    print('Updating Report...')
  
    ###################  PROCESSED FILE  ############################
    # Create New Processed File if 1 or more rows
    if df_file.shape[0] > 0:
        new_filename = filename[:-4] + '-processed' + filename[-4:]
        Output_filename = Output_path_processed_csv + new_filename
        df_file.to_csv(path_or_buf=Output_filename, sep='|', index=False)
        print('Creating a Processed File')
    
    ###################  CREATE NO SPT FILE  ############################
    # Create New Bad File for fixes that can't be made
    if (Bad_sptz == True):
        if df_cant_load.shape[0] > 0:
            new_filename = filename[:-4] + '-NoSPTs' + filename[-4:]
            Output_filename = Output_path_badSPT + new_filename
            df_cant_load.to_csv(path_or_buf=Output_filename, sep='|', index=False)
            print('Creating a Bad Location Code File')

    ###################  CREATE UNMAPPED TKC FILE  ############################
    # Create New TKC File for data with unmapped TKCs
    if (Bad_TKCs == True):
        if df_cant_load_TKCs.shape[0] > 0:
            new_filename = filename[:-4] + '-UnTKCs' + filename[-4:]
            Output_filename = Output_path_badTKC + new_filename
            df_cant_load_TKCs.to_csv(path_or_buf=Output_filename, sep='|', index=False)
            print('Creating an Unmapped TKC file')
            
    ###################  CREATE RETESTS FILE  ############################
    if (Retests_In_File == True):
        if df_Retests.shape[0] > 0:
            new_filename = filename[:-4] + '-Retests' + filename[-4:]
            Output_filename = Output_path_Retests + new_filename
            df_Retests.to_csv(path_or_buf=Output_filename, sep='|', index=False)
            print('Creating a Retest File')

# CREATE EXCEL REPORT------------------------------------------------------------------------------------
#region
# Generate Excel Report
print('Creating Excel Report')
Report_path_filename = Output_path_Report + 'Report.xlsx'
writer = pd.ExcelWriter(Report_path_filename)
df_Report.to_excel(writer,'Sheet1')
writer.save()
#endregion

print('ALL DONE!')
#End 7.1
#endregion

# 7.2 Swapping items in a column only, using a lookup table
#region
# Imporint Libs
#region
print('Importing LIBRARIES')
import pandas as pd
import numpy as np
import os
import re
import glob
import csv
import shutil
print('Libs Imported')
#endregion
# INPUT VARIABLES----------------------------------------------------------------------------------------
#region
print('Importing Input variables')
# Directory folder of the csv files you want to process
Input_path_CSVs = 'C:/FILES/Input_CSV/'
# Column Name to Apply Swapping To
Col_To_Apply_Swap = 'LOCATIONCODE'
# Directory folder of the report
Output_path_Report = 'C:/FILES/'
# Can change to xlsx if needed, other changes will be nessesary to code
Extension = 'csv'
# Csv files seperator for input and output files..generally (,) or (|)
Delimiter = '|'
# Directory excel file of the Sample Point Table
Input_path_SPT = 'C:/FILES/Sample Points.xlsx'
# Output folder of the CSV files
Output_path_processed_csv = 'C:/FILES/Output_CSV_Processed/'
# Output folder path of CSV Files with Structure that can't be Analysed
Output_path_bad_structure = 'C:/FILES/Output_CSV_Bad_Column_Structure/'
# Output folder path of Report on Analysed files
Output_path_Report = 'C:/FILES/'
print('Directories loaded...')
#endregion
# READ AND PROCESS THE UNIQUE SAMPLE POINTS FILE----------------------------------------------------------------
#region
df_SPTs = pd.read_excel(Input_path_SPT, sheet_name='Data', dtype=object)
List_Columns_Keep = ['Name','OldSiteCode_post2007', 'Status']
df_SPTs = df_SPTs[List_Columns_Keep]
df_SPTs.columns = ['REPLACE-WITH', 'FIND-CODE', 'Status']
# Remove Inactive points
df_SPTs = df_SPTs[~df_SPTs['Status'].astype(str).str.match('Inactive')]
# Delete non Unique (duplicate) OSC's
df_SPTs.drop_duplicates(subset='FIND-CODE', keep=False, inplace=True)
# Create a list of Unique OSC's
List_OSCs = df_SPTs['FIND-CODE'].tolist()
# Delete the Desc and Status Columns
List_Columns_Keep = ['REPLACE-WITH', 'FIND-CODE']
df_SPTs = df_SPTs[List_Columns_Keep]
# Change index to old site code
df_SPTs.set_index('FIND-CODE', inplace = True)
dict_SPTs = df_SPTs.to_dict()
dict_SPTs = dict_SPTs.get('REPLACE-WITH')

print('SPT Cross Reference Table created...')
#endregion
# SAVE CSV FILENAMES IN A LIST AND DATAFRAME-------------------------------------------------------------
#region
# Get the csv filenames into an array
os.chdir(Input_path_CSVs)
filenames = [i for i in glob.glob('*.{}'.format(Extension))]

# Get the number of csv files
NumFiles = len(filenames)
print(NumFiles, 'csv files found.')
#endregion

# MOVE FILES WITHOUT THE NAME OF THE COLUMN TO SWAP  --------------------------------------
#region
counter_good_files = 0
counter_bad_files = 0
List_Unsupported_Files = []
for filename in filenames:
    # Save an individual file as a DataFrame Object to analyse
    try:
        df_file = pd.read_csv(filename, sep=Delimiter, engine='python', dtype=object)
        Column_Names_List = df_file.columns.tolist()
        if Col_To_Apply_Swap in Column_Names_List:
            counter_good_files +=1
        else:
            List_Unsupported_Files.append(filename)
            counter_bad_files +=1
    except:
        # Try and deal with weirdly formatted csv
        try:
            df_file = pd.read_csv(filename, sep=Delimiter, engine='c', dtype=object, encoding='latin1')
            Column_Names_List = df_file.columns.tolist()
            if Col_To_Apply_Swap in Column_Names_List:
                counter_good_files +=1
            else:
                List_Unsupported_Files.append(filename)
                counter_bad_files +=1
        except:    
            List_Unsupported_Files.append(filename)
            counter_bad_files +=1

# Print stats        
print('Number of Files that can be Analysed: ', counter_good_files)
print('Number of Files that can\'t be Analysed: ', counter_bad_files)

# Move files
print('Moving Files...')
files = os.listdir(Input_path_CSVs)

for f in files:
    if f in List_Unsupported_Files:
        shutil.move(f, Output_path_bad_structure)


# Get the csv filenames into an array after unwanted ones are moved
os.chdir(Input_path_CSVs)
filenames = [i for i in glob.glob('*.{}'.format(Extension))]

#endregion

# CREATE AN EMPTY DATAFRAME FOR REPORT-------------------------------------------------------------------
#region
List_Columns = ['Filename', 'Total Rows', 'Replaced SPT Codes']
df_Report = pd.DataFrame(columns=List_Columns)
#endregion

# LOOP THOUGH EACH FILE AND PROCESS IT ------------------------------------------------------------------

for filename in filenames:
    print('-----------------------------------------------')
    print('current file:')
    print(filename)
    

    # Set Booleans
    Bad_sptz = False
    
    # Set Counts
    Int_Total_Rows = int(0)
    Int_Replaced_SPTs = int(0)
    
    # Save the individual file as a DataFrame Object to analyse
    try:
        df_file = pd.read_csv(filename, sep=Delimiter, index_col=False, engine='python', dtype=object)
    except:
        df_file = pd.read_csv(filename, sep=Delimiter, index_col=False, engine='c', dtype=object, encoding='latin1')
    # Delete Rows with everything missing in the row
    df_file = df_file.dropna(axis='index', how='all')
    Int_Total_Rows = df_file.shape[0]
    print('Rows in file: ', Int_Total_Rows)

    ###################  QA DATA DELETION  ##################################
    # Check and Find if Blanks exist in Location Code Rows
    bools_ml = df_file[Col_To_Apply_Swap].isnull()
    bools_ml = np.array(bools_ml)
    
    # Check and Find if/Where Blanks exist in Location Description Rows
    bools_md = df_file['LocationDescription'].isnull()
    bools_md = np.array(bools_ml)

       
    ################### SWAP OLD CODES FOR SPT CODES ####################
        # Count the number of replacements that can be made
    for item in df_file[Col_To_Apply_Swap]:
        if item in List_OSCs:
            Int_Replaced_SPTs +=1

    # Make the replacements
    df_file.rename(columns={Col_To_Apply_Swap: 'TEMP_SWAP_NAME'}, inplace=True)
    df_file.TEMP_SWAP_NAME.replace(dict_SPTs , inplace = True)
    df_file.rename(columns={'TEMP_SWAP_NAME':Col_To_Apply_Swap}, inplace=True)
      
    
    ###################  REPORT UPDATING ############################
    # Append Update the Report Dataframe
    print('Updating Report...')
    List_Row_Report = [filename, Int_Total_Rows, Int_Replaced_SPTs]
    df_Temp_Report = pd.DataFrame([List_Row_Report], columns=List_Columns)
    df_Report = df_Report.append(df_Temp_Report, ignore_index=True)
    print('Report Updated.')
    print('Rows in file: ', Int_Total_Rows)
    print('Replaced Items: ', Int_Replaced_SPTs)
    
  
    ###################  PROCESSED FILE  ############################
    # Create New Processed File if 1 or more rows
    print('Creating a Processed File...')
    if df_file.shape[0] > 0:
        new_filename = filename[:-4] + '-processed' + filename[-4:]
        Output_filename = Output_path_processed_csv + new_filename
        df_file.to_csv(path_or_buf=Output_filename, sep='|', index=False)
        print('Created Processed File')
               
# CREATE EXCEL REPORT-------------------------------------------------------------
#region
# Generate Excel Report
print('Creating Excel Report')
Report_path_filename = Output_path_Report + 'Report.xlsx'
writer = pd.ExcelWriter(Report_path_filename)
df_Report.to_excel(writer,'Sheet1')
writer.save()
#endregion

print('ALL DONE!')

#End 7.2
#endregion
