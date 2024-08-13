import pandas as pd

# Load the Excel file
input_file = 'data.xlsx'  # Replace with your input file name
output_file_cleaned = 'output_file_cleaned.xlsx'  # Replace with your desired output file name for cleaned data
output_file_duplicates = 'output_file_duplicates.xlsx'  # Replace with your desired output file name for duplicates

# Read the Excel file
df = pd.read_excel(input_file)

# Identify duplicate rows based on "Prospect_Number" column
duplicates = df[df.duplicated(subset='Prospect_Number', keep=False)]

# Remove duplicate rows based on "Prospect_Number" column
df_cleaned = df.drop_duplicates(subset='Prospect_Number')

# Write the cleaned data to a new Excel file
df_cleaned.to_excel(output_file_cleaned, index=False)

# Write the duplicate rows to a new Excel file
duplicates.to_excel(output_file_duplicates, index=False)

print(f"Cleaned data has been saved to {output_file_cleaned}")
print(f"Duplicate rows have been saved to {output_file_duplicates}")
