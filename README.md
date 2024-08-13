# SMS PDF GENRATOR

## Description

This Python script automates the processing of data from an Excel spreadsheet (lang_data.xlsx) to generate PDF documents using templates tailored to different states and languages. It then compresses these PDFs and uploads them to an AWS S3 bucket, optionally creating new folders based on user input.

## Environment Variables

The script uses environment variables to manage AWS credentials and configurations. Make sure to have a .env file in your project directory with the following variables:

- `S3_BUCKET_NAME`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_URI`

## State Template Dictionary

A dictionary named `state_template_dict` is used to store states categorized by their official languages. Each state is set to None by default and can be updated as needed.

## `configure_logging` Function

The configure_logging function sets up logging for the script. It creates separate handlers for INFO and ERROR messages, which are logged to separate files.

- Function: configure_logging
- Parameters:
  - `info_log_file`: Name of the file to log INFO messages.
  - `error_log_file`: Name of the file to log ERROR messages.

## `Terminating_Microsoft_Word_Instances` Function

The terminate_word_instances function checks for running instances of Microsoft Word (WINWORD.EXE) and terminates them. It logs the process IDs (PIDs) of the running instances before terminating them.

- Output:
  - Logs the PIDs of running Microsoft Word instances.
  - Terminates each instance.
  - Logs a message indicating all instances have been terminated.

## `generate_pdf_from_docx` Function

This function converts all DOCX files in a specified directory to PDF format and then deletes the original DOCX files.

- Parameters:

  - output_dir (str): The directory containing the DOCX files to be converted.

- Usage:

  - generate_pdf_from_docx(output_dir)

- Steps
  - Convert DOCX files to PDF using the convert function.
  - List all DOCX files in the specified directory.
  - Delete each DOCX file after conversion.
  - Print a confirmation message for each deleted file.

## `process_record` Function

This function processes a single record, generating a DOCX file based on a state-specific template and saving it to the specified output directory.

- Parameters:

  - record (dict): The record containing data to be merged into the template.
  - output_dir (str): The directory where the generated DOCX files will be saved.
  - state_template_dict (dict): A dictionary mapping state names to their corresponding template file paths.
  - potential_column_names (list): A list of potential column names to be used for generating the file name. Defaults to ['Filename', 'Prospect_no', 'LAN_Details', 'CUID_NO'].

- Returns:

  - file_name (str): The name of the generated file (without extension) if successful, None otherwise.
  - state (str): The state associated with the record if successful, None otherwise.

- Steps:

  1. Extract the state from the record and convert it to uppercase.
  2. Retrieve the corresponding template path from state_template_dict. If no template is found, use the default template.
  3. Load the template and render it with the data from the record.
  4. Determine the file name using the first valid column name from potential_column_names.
  5. Save the rendered document to the specified output directory.
  6. Log errors if any occur during the process.

## `process_lang_folder` Function

This function processes a folder containing language-specific data, converts Excel data into DOCX files using state-specific templates, and manages multiple Microsoft Word instances to optimize performance.

- Parameters:

  - lang_folder (str): The path to the folder containing the language-specific data.
    output_dir (str): The directory where the generated DOCX files will be saved.
  - state_template_dict (dict): A dictionary mapping state names to their corresponding template file paths.
  - num_word_instances (int, optional): The number of Microsoft Word instances to create. Default is 3.

- Steps:

  1. Read the Excel file lang_data.xlsx from the specified lang_folder into a pandas DataFrame.
  2. Create multiple Microsoft Word instances using comtypes.
  3. Use a ThreadPoolExecutor to process each row of the DataFrame concurrently, calling the process_record function.
  4. Close all Microsoft Word instances after processing.

## `merge_pdfs_by_srno` Function

This function merges PDFs based on sequential numbers (SrNo) from an Excel sheet, creating batches of merged PDFs.

- Parameters:

  - output_dir (str): Directory containing the individual PDF files to be merged.
  - base_dir (str): Base directory where the merged PDF files will be saved.
  - excel_path (str): Path to the Excel sheet with the 'SrNo' column.
  - potential_column_names (list): A list of potential column names in the Excel sheet that contain the filenames.
  - batch_size (int, optional): The number of SrNo values to process in each batch. Default is 5000.

- Steps:

  1. Read the Excel sheet into a pandas DataFrame.
  2. Create a mapping of SrNo to PDF filenames based on the provided column names.
  3. Sort the SrNo values.
  4. Process the SrNo values in batches, merging the corresponding PDFs using PdfMerger.
  5. Save each merged PDF with a filename indicating the range of SrNo values.
  6. Log any errors encountered during the process.

## `check_and_update_template` Function

This function checks for the existence of specific template files for a given state and updates the state_template_dict accordingly. If no specific template is found, it defaults to using a common template.

- Parameters:

  - state (str): The state for which the template needs to be checked.
  - possible_files (list): A list of possible template file names to check.
  - default_template (str, optional): The default template file name. Default is "default_template.docx".
  - script_dir (str, optional): The directory where the script is located. Default is the directory of the current script.
  - template_dict (dict, optional): The dictionary mapping states to their template file paths. Default is state_template_dict.

## `check_missing_pdfs` Function

This function checks for missing PDF files based on the data in an Excel sheet and logs any missing files.

- Parameters:

  - lang_folder (str): The path to the folder containing the language-specific data.
  - output_dir (str): The directory where the generated PDF files should be saved.
  - potential_column_names (list): A list of potential column names in the Excel sheet that contain the filenames.

- Returns:
  - A list of missing PDF filenames, or None if all PDFs are generated successfully.

## `process_missing_pdfs` Function

This function processes the missing PDFs by re-generating DOCX files and converting them to PDFs.

- Parameters:

  - lang_folder (str): The path to the folder containing the language-specific data.
  - output_dir (str): The directory where the generated PDF files should be saved.
  - missing_pdfs (list): A list of missing PDF filenames.
  - state_template_dict (dict): A dictionary mapping state names to their corresponding template file paths.

## `upload_to_s3` Function

This function uploads a local file to an AWS S3 bucket.

- Parameters:

  - local_file_path (str): The local path to the file to be uploaded.
  - bucket (str): The name of the S3 bucket.
  - s3_file_name (str): The name to assign to the file in S3.
  - aws_access_key_id (str, optional): AWS access key ID.
  - aws_secret_access_key (str, optional): AWS secret access key.
  - aws_region (str, optional): AWS region.

- Returns:
  - bool: True if the upload was successful, False otherwise.

## `uploading_to_s3` Function

This function uploads multiple files from a local directory to an S3 bucket and handles any failures by copying the failed files to a specified directory.

- Parameters:

  - output_dir (str): The local directory containing the files to be uploaded.
  - s3_uri (str): The S3 URI prefix to use for the uploaded files.
  - s3_bucket_name (str): The name of the S3 bucket.
  - aws_access_key_id (str): AWS access key ID.
  - aws_secret_access_key (str): AWS secret access key.
  - aws_region (str): AWS region.
  - failed_files_dir (str): The local directory to copy failed files to.

## `get_count_of_files_in_bucket` Function

This function counts the number of files in a specified folder within an S3 bucket.

- Parameters:

  - aws_access_key_id (str, optional): AWS access key ID.
  - aws_secret_access_key (str, optional): AWS secret access key.
  - aws_region (str, optional): AWS region.
  - s3_bucket_name (str): The name of the S3 bucket.
  - folder_name (str): The name of the folder in the S3 bucket to count files in.

## `create_s3_folder` Function

This function creates a folder in an AWS S3 bucket. The folder name is based on the current date in the format "DDMM".

- Parameters:

  - bucket_name (str): The name of the S3 bucket.

- Returns:
  - str: The name of the folder created.

## `compressing_pdf` Function

This function compresses PDF files in the specified output directory using a batch file.

- Parameters:

  - output_dir (str): The directory where the PDF files are located.
  - bat_file_path (str): The path to the batch file used for compression.
  - compress_dir (str): The directory where the compressed PDF files will be saved.

## `delete_directory` Function

This asynchronous function attempts to delete a directory and its contents, retrying up to a specified number of times if the deletion fails.

- Parameters:

  - directory_path (str): The path of the directory to delete.
  - max_retries (int, optional): The maximum number of retries. Default is 3.
  - retry_delay (int, optional): The delay between retries in seconds. Default is 10.

## `log_initial_info` Function

This function logs initial information about the total number of records, total batches, and batch size.

- Parameters:

  - total_records (int): The total number of records.
  - total_batches (int): The total number of batches.
  - chunk_size (int): The size of each batch.

## `get_user_input` Function

This function prompts the user for input and returns True if the user enters "yes" and False if the user enters "no". It continues to prompt the user until a valid input is received.

- Parameters:

  - prompt (str): The prompt message to display to the user.

- Returns:
  - bool: True if the user enters "yes", False if the user enters "no".

## `log_state_counts` Function

This function logs the total number of records for each state.

- Parameters:

  - state_counts_total (dict): A dictionary where the keys are state names and the values are the counts of records for each state.

## `main()` Function

The `main()` function orchestrates the entire workflow of data processing, PDF generation, compression, and AWS S3 integration.

#### Workflow

1. **Setup**: Creates necessary output directories (`OUTPUT`, `COMPRESS`, `UPLOAD_FAIL_FILES`).

2. **Data Loading**: Loads data from `lang_data.xlsx` located in the `lang` folder.

3. **Template Checking**: Ensures availability and updates templates for various states and languages.

4. **User Interaction**: Prompts the user for inputs to determine script behavior (`running_main_code`, `create_new_s3_folder`, etc.).

5. **Processing**: Depending on user inputs, processes data:

   - Generates PDFs in batches.
   - Checks for missing PDFs and processes them if found.
   - Compresses generated PDFs.

6. **AWS S3 Integration**: Uploads compressed files to AWS S3. Allows creating a new folder or using an existing one based on user input.

7. **Logging**: Logs key events and decisions made during script execution.

#### User Inputs

- `running_main_code`: Determines whether to execute the main data processing workflow or specific tasks based on user input.
- Various other inputs (`create_new_s3_folder`, `existing_folder_name`, etc.) influence how the script interacts with AWS S3.

#### Error Handling

- Handles exceptions and logs warnings or errors appropriately.

#### Notes

- Ensure `lang_data.xlsx` is correctly formatted and accessible.
- Verify AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) before executing script.
