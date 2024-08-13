# import os
# import pandas as pd

# # Define the paths
# excel_path = 'main.xlsx'
# pdf_folder_path = 'photo'
# output_excel_path = 'missing_files.xlsx'

# # Read the Excel file
# df = pd.read_excel(excel_path)

# # Extract the Prospect_Number column
# prospect_numbers = df['Prospect_Number'].astype(str).tolist()

# # List all PDF files in the folder
# pdf_files = [f.replace('.pdf', '') for f in os.listdir(pdf_folder_path) if f.endswith('.pdf')]

# # Find missing prospect numbers
# missing_files = [num for num in prospect_numbers if num not in pdf_files]

# # Create a DataFrame for missing files
# missing_df = pd.DataFrame(missing_files, columns=['Missing_Prospect_Number'])

# # Save the missing files to a new Excel file
# missing_df.to_excel(output_excel_path, index=False)

# print(f'Missing files have been saved to {output_excel_path}')


import os
import pandas as pd

# Define the paths
excel_path = 'lang_data.xlsx'
pdf_folder_path = 'OUTPUT'
output_excel_path = 'missing_files.xlsx'

# Read the Excel file
df = pd.read_excel(excel_path)

# Extract the Prospect_Number column
prospect_numbers = df['barcode'].astype(str).tolist()

# List all PDF files in the folder
pdf_files = [f.replace('.pdf', '') for f in os.listdir(pdf_folder_path) if f.endswith('.pdf')]

# Find missing prospect numbers
missing_files = [num for num in prospect_numbers if num not in pdf_files]

# Filter rows in the original dataframe where Prospect_Number is in the missing files
missing_df = df[df['barcode'].astype(str).isin(missing_files)]

# Save the missing files' data to a new Excel file
missing_df.to_excel(output_excel_path, index=False)

print(f'Missing files data have been saved to {output_excel_path}')
