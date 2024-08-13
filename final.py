import os
from concurrent.futures import as_completed
import pandas as pd
from docxtpl import DocxTemplate
import comtypes.client
from concurrent.futures import ThreadPoolExecutor
import psutil
from docx2pdf import convert
from PyPDF2 import PdfMerger
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import subprocess
import shutil
from dotenv import load_dotenv
import logging
import traceback
import asyncio
from datetime import datetime


load_dotenv()

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_URI = os.getenv("S3_URI")
URI = os.getenv("URI")



state_template_dict = {
    # Hindi
    'BIHAR': None,
    'DELHI': None,
    'HARYANA': None,
    'JHARKHAND': None,
    'HIMACHAL PRADESH': None,
    'MADHYA PRADESH': None,
    'RAJASTHAN': None,
    "CHHATTISGARH": None,
    'UTTAR PRADESH': None,
    'UTTARAKHAND': None,

    # Bengali
    'WEST BENGAL': None,
    'TRIPURA': None,
    
    # Assam
    'ASSAM': None,

    # Gujrati
    'GUJARAT': None,

    # Karnataka
    'KARNATAKA': None,

    # Marathi
    'GOA': None,
    'MAHARASHTRA': None,

    # Orissa
    'ORISSA': None,

    # Punjabi
    'PUNJAB': None,

    # Tamil-Nadi
    'TAMIL NADU': None,

    # Andhra-Pradesh
    'ANDHRA PRADESH': None, 

    # Kerala
    'KERALA': None,

    # Default template
    'DEFAULT': None
}




def configure_logging(info_log_file, error_log_file):
    # Basic configuration for logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(message)s')

    # Create separate handlers for INFO and ERROR messages
    info_handler = logging.FileHandler(info_log_file)
    error_handler = logging.FileHandler(error_log_file)

    # Set the logging level for each handler
    info_handler.setLevel(logging.INFO)
    error_handler.setLevel(logging.ERROR)

    # Create formatters for each handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # formatter = logging.Formatter('%(message)s')
    info_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    # Add the handlers to the root logger
    logging.getLogger().addHandler(info_handler)
    logging.getLogger().addHandler(error_handler)


info_log_file = "info_log.txt"
error_log_file = "error_log.txt"
configure_logging(info_log_file, error_log_file)


def terminate_word_instances():
    # Check for running Microsoft Word instances
    word_processes = [proc for proc in psutil.process_iter() if "WINWORD.EXE" in proc.name()]

    if word_processes:
        logging.info("Microsoft Word instances running:")
        for proc in word_processes:
            logging.info(f"PID: {proc.pid}")

        # Terminate all Microsoft Word instances
        for proc in word_processes:
            proc.terminate()
            print(f"Terminated PID: {proc.pid}")
            logging.info(f"Terminated PID: {proc.pid}")

        logging.info("------------------------------ All Microsoft Word instances terminated. ------------------------ \n")
    else:
        logging.info("No Microsoft Word instances are running.")



def generate_pdf_from_docx(output_dir):

    # Generate PDF from docx files
    convert(output_dir)

    # Delete docx files
    word_files = [f for f in os.listdir(output_dir) if f.lower().endswith('.docx')]
    for word_file in word_files:
        word_file_path = os.path.join(output_dir, word_file)
        os.remove(word_file_path)
        print(f"---------------- Deleted Word file: {word_file_path} ----------------------\n")
        


def process_record(record, output_dir, state_template_dict, potential_column_names=['Filename', 'Prospect_no', 'LAN_Details', 'CUID_NO']):
    try:
        file_name = None  # Move the definition outside the try block

        state = record.get('State', '').upper()
        template_path = state_template_dict.get(state, state_template_dict.get('DEFAULT', os.path.join(os.path.dirname(__file__), 'default_template.docx')))

        if not os.path.exists(template_path):
            logging.info(f"No template found for state: {state}. Using default template.")
            template_path = state_template_dict.get('DEFAULT', os.path.join(os.path.dirname(__file__), 'default_template.docx'))

        doc = DocxTemplate(template_path)
        doc.render(record)

        # Try each potential column name until a valid one is found
        file_name = next((record.get(key, None) for key in potential_column_names if key in record), None)

        if file_name is None:
            logging.error("No valid column found for file name. Unable to generate file name.")
            return None

        output_path = os.path.join(output_dir, f"{file_name}.docx")
        doc.save(output_path)

        return file_name, state

    except Exception as e:
        error_message = f"Error occurred for file {record.get(file_name, 'unknown')}: {e}\n"  # Use the variable directly instead of file_name
        logging.error(error_message)
        return None, None
    


def process_lang_folder(lang_folder, output_dir, state_template_dict, num_word_instances=3):

    excel_path = os.path.join(lang_folder, "lang_data.xlsx")

    # Convert Excel sheet to pandas dataframe
    df = pd.read_excel(excel_path, sheet_name="Sheet1")

    # Create a pool of Word instances
    word_instances = [comtypes.client.CreateObject('Word.Application') for _ in range(num_word_instances)]

    with ThreadPoolExecutor() as executor:
        # Pass state_template_dict as an additional argument to process_record function
        futures = [executor.submit(process_record, row, output_dir, state_template_dict) for _, row in df.iterrows()]
        
        for future in futures:
            file_name = future.result()
            
    # Close all Word instances
    for word_instance in word_instances:
        word_instance.Quit()



def merge_pdfs_by_srno(output_dir, base_dir, excel_path, potential_column_names, batch_size=5000):
    """
    Merge PDFs based on SrNo from an Excel sheet.

    Parameters:
    - output_dir (str): Directory to store the merged PDFs.
    - base_dir (str): Base directory for saving the merged PDFs.
    - excel_path (str): Path to the Excel sheet with 'SrNo' column.
    - batch_size (int, optional): Number of SrNo to process in each batch. Default is 5000.
    """

    # Read Excel sheet with 'SrNo' column
    try:
        df = pd.read_excel(excel_path, sheet_name="Sheet1")
    except pd.errors.EmptyDataError:
        logging.error("Error: Empty Excel sheet.")
        return

    # Create a mapping of SrNo to PDF filenames
    srno_to_pdf = {str(row['SrNo']): f"{row[column]}.pdf" for _, row in df.iterrows() for column in potential_column_names if column in row}

    # Sort SrNo values
    sorted_srnos = sorted(srno_to_pdf.keys(), key=int)

    total_pdfs = len(sorted_srnos)

    start_idx = 0
    end_idx = min(batch_size, total_pdfs)

    while start_idx < total_pdfs:
        pdf_merger = PdfMerger()

        # Initialize variables with default values
        merged_start_srno = None
        merged_end_srno = None

        try:
            for srno in sorted_srnos[start_idx:end_idx]:
                pdf_file = srno_to_pdf.get(str(srno))
                if pdf_file:
                    pdf_file_path = os.path.join(output_dir, pdf_file)
                    pdf_merger.append(pdf_file_path)
                else:
                    logging.warning(f"Skipping SrNo {srno} - PDF file not found.")

            # Adjust the merged PDF file name
            merged_start_srno = sorted_srnos[start_idx]
            merged_end_srno = sorted_srnos[end_idx - 1]
            merged_pdf_path = os.path.join(base_dir, f'merged_output_srno_{merged_start_srno}-{merged_end_srno}.pdf')

            with open(merged_pdf_path, 'wb') as merged_pdf:
                pdf_merger.write(merged_pdf)

            logging.info(f"Merged PDFs {merged_start_srno} to {merged_end_srno} based on SrNo successfully. Merged PDF saved at: {merged_pdf_path}")

        except FileNotFoundError as fnfe:
            logging.warning(f"File not found while merging PDFs {merged_start_srno} to {merged_end_srno} based on SrNo: {fnfe}")
        except Exception as e:
            logging.error(f"Error merging PDFs {merged_start_srno} to {merged_end_srno} based on SrNo: {e}")

        finally:
            pdf_merger.close()

        start_idx += batch_size
        end_idx = min(end_idx + batch_size, total_pdfs)

    logging.info(" ------------------- All PDFs merged based on SrNo successfully. ------------------------ \n")



def check_and_update_template(state, possible_files, default_template="default_template.docx", script_dir=os.path.dirname(os.path.abspath(__file__)), template_dict=state_template_dict):
    # Avoid redundant path concatenation
    lang_path = os.path.join(script_dir, 'lang')

    # Check for the existence of each file and update the state_template_dict
    for file_name in possible_files:
        file_path = os.path.join(lang_path, file_name)
        if os.path.exists(file_path):
            template_dict[state] = file_path
            break  # Stop searching once a file is found
    else:
        # If none of the specific files is found, use the default template
        template_dict[state] = os.path.join(lang_path, default_template)



def check_missing_pdfs(lang_folder, output_dir, potential_column_names):
    excel_path = os.path.join(lang_folder, "lang_data.xlsx")
    df = pd.read_excel(excel_path, sheet_name="Sheet1")

    # Create a list to store missing PDF documents
    missing_pdfs = []

    for _, row in df.iterrows():

        # Try each potential file name until a valid one is found
        file_name = next((row.get(name, '') for name in potential_column_names if row.get(name, '')), None)

        if file_name:
            pdf_file_path = os.path.join(output_dir, f"{file_name}.pdf")

            # Check if the generated PDF file exists
            if not os.path.exists(pdf_file_path):
                missing_pdfs.append(file_name)

    if missing_pdfs:
        # Log missing PDFs as errors
        logging.error("Missing PDFs:")
        for missing_pdf in missing_pdfs:
            logging.error(f"- {missing_pdf}")

        logging.error("End of Missing PDFs")

        # Return the list of missing PDFs
        return missing_pdfs
    else:
        print("All PDFs generated successfully.")
        return None



def process_missing_pdfs(lang_folder, output_dir, missing_pdfs, state_template_dict):

    if not missing_pdfs:
        print("No missing PDFs to process.")
        return

    excel_path = os.path.join(lang_folder, "lang_data.xlsx")
    df = pd.read_excel(excel_path, sheet_name="Sheet1")

    # Iterate over columns to find a column that contains the missing filenames
    matching_columns = [col for col in df.columns if df[col].isin(missing_pdfs).any()]

    if not matching_columns:
        print("No columns found containing missing PDF filenames.")
        return

    # Filter DataFrame to include only missing PDFs
    missing_pdfs_df = df[df[matching_columns[0]].isin(missing_pdfs)]

    try:
        with ThreadPoolExecutor() as executor:
            # Pass state_template_dict as an additional argument to process_record function
            futures = [executor.submit(process_record, row, output_dir, state_template_dict) for _, row in missing_pdfs_df.iterrows()]

            # Wait for all tasks to complete
            for future in as_completed(futures):
                try:
                    file_name = future.result()
                    if file_name:
                        generate_pdf_from_docx(output_dir)

                except Exception as conversion_error:
                    print(f"Error during conversion: {conversion_error}")

    except Exception as e:
        print(f"An error occurred: {e}")
        # Log the error for further investigation
        logging.error(f"An error occurred during PDF processing: {e}")



def upload_to_s3(local_file_path, bucket, s3_file_name, aws_access_key_id=None, aws_secret_access_key=None, aws_region=None):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

    try:
        s3.upload_file(
            local_file_path,
            bucket,
            s3_file_name,
            ExtraArgs={'ContentType': 'application/pdf', 'ContentDisposition': 'inline'}
        )
        logging.info(f"File uploaded successfully to {bucket}/{s3_file_name}")
        return True
    except FileNotFoundError:
        logging.error("The file was not found")
    except NoCredentialsError:
        logging.error("Credentials not available")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    return False



def uploading_to_s3(output_dir, s3_uri, s3_bucket_name, aws_access_key_id, aws_secret_access_key, aws_region, failed_files_dir):
    logging.info("Processing S3 upload...")
    local_file_paths = [os.path.join(output_dir, pdf_file) for pdf_file in os.listdir(output_dir)]

    success_count = 0
    failed_files = []

    for local_pdf_path in local_file_paths:
        s3_file_name = f"{s3_uri}/{os.path.basename(local_pdf_path)}"
        success = upload_to_s3(local_pdf_path, s3_bucket_name, s3_file_name, aws_access_key_id, aws_secret_access_key, aws_region)
        
        if success:
            success_count += 1
        else:
            failed_files.append(local_pdf_path)

    logging.info(f"--------------- {success_count} files uploaded successfully to S3 -----------------")
    
    if failed_files:
        logging.warning(f"--------------- {len(failed_files)} files failed to upload to S3 -----------------")
        for failed_file in failed_files:
            logging.warning(f"Failed to upload: {failed_file}")
            # Copy failed files to the specified directory
            shutil.copy(failed_file, os.path.join(failed_files_dir, os.path.basename(failed_file)))
    else:
        logging.info("All files uploaded successfully to S3.\n")



def get_count_of_files_in_bucket(aws_access_key_id=None, aws_secret_access_key=None, aws_region=None, s3_bucket_name=None, folder_name=None) :
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)


    # Initialize variables
    file_count = 0
    continuation_token = None

    # Paginate through the results to count the number of files
    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=s3_bucket_name,
                Prefix=folder_name,
                ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(
                Bucket=s3_bucket_name,
                Prefix=folder_name
            )
        
        # Count the number of files in the current response
        file_count += len(response.get('Contents', []))

        # Check if there are more results to paginate through
        if response.get('NextContinuationToken'):
            continuation_token = response['NextContinuationToken']
        else:
            break

    print(f"Number of files in folder '{folder_name}': {file_count}")



def create_s3_folder(bucket_name):
    try:
        # Get today's date in the format DDMM
        today = datetime.today()
        folder_name = URI+"/"+today.strftime('%d%m')
        
        # Create an S3 client
        s3 = boto3.client('s3')
        
        # Create a "folder" in S3 by creating a zero-byte object with a trailing slash
        folder_key = folder_name + '/'
        s3.put_object(Bucket=bucket_name, Key=folder_key)
        
        # Log that the folder is created successfully
        logging.info(f"Folder created successfully in {bucket_name}")
        logging.info(f"Folder name: {folder_name}")
        logging.info(f"s3 URI: {bucket_name}/{folder_name}")
        
        # Construct the URL for the file in the new folder
        file_url = folder_name
        
        return file_url
    
    except Exception as e:
        # Log the error
        logging.error(f"Error creating folder: {e}")
        # Raise the exception again to propagate it to the caller
        raise



def process_dataframe_in_batches(df, chunk_size, total_batches, batch, output_dir, state_template_dict, potential_column_names):

    for i in range(0, len(df), chunk_size):
        df_chunk = df.iloc[i:i + chunk_size]

        # Sort the chunk DataFrame by 'State'
        df_chunk = df_chunk.sort_values(by='State')

        logging.info(f"------------ Processing batch {i//chunk_size + 1} / {total_batches}")
        batch += 1

        state_counts_chunk = df_chunk['State'].value_counts().to_dict()
        logging.info(f"------------ States in batch {i//chunk_size + 1}:")
        for state, count in sorted(state_counts_chunk.items(), key=lambda x: x[0]):
            logging.info(f"{state}: {count}")

        # Call process_record for each row in the chunk
        for _, record in df_chunk.iterrows():
            process_record(record, output_dir, state_template_dict, potential_column_names)

        # Convert generated DOCX files to PDFs
        terminate_word_instances()
        generate_pdf_from_docx(output_dir)

        logging.info(f"End of batch {i//chunk_size + 1}")
        logging.info("-------------------------------\n")



def compressing_pdf(output_dir, bat_file_path, compress_dir): 
    logging.info(f"----------------------- Compressing pdf... -------------------------------\n")

    # Save the current working directory
    current_dir = os.getcwd()

    try:
        # Change directory to the output directory where the PDFs are generated
        os.chdir(output_dir)

        subprocess.run(bat_file_path)

        logging.info(f"--------------- All PDF compressed successfully... -----------------------\n")

        pdf_files = [file for file in os.listdir(compress_dir) if file.endswith(".pdf")]
        logging.info(f"Number of PDFs in COMPRESS directory: {len(pdf_files)}")
    except Exception as e:
        logging.error(f"Error while compressing PDFs: {e}")
    finally:
        # Change back to the original directory
        os.chdir(current_dir)



async def delete_directory(directory_path, max_retries=3, retry_delay=10):
    for _ in range(max_retries):
        try:
            # Attempt to remove the directory and its contents
            shutil.rmtree(directory_path)
            logging.info(f"Directory '{directory_path}' successfully deleted.")
            return  # Exit the function if deletion is successful
        except OSError as e:
            # Handle any errors that may occur during the deletion
            logging.error(f"Error: {e}")
            logging.error("Failed to delete directory. Retrying...")
            traceback.print_exc()  # Print detailed error information
            await asyncio.sleep(retry_delay)
    logging.error("Failed to delete directory after multiple attempts.")



def log_initial_info(total_records, total_batches, chunk_size):
    logging.info(f"Total records: {total_records}")
    logging.info(f"Total batches: {total_batches}")
    logging.info(f"Batch size: {chunk_size}")
    logging.info("-------------------------------------------------------------\n")



def get_user_input(prompt):
    while True:
        user_input = input(prompt).strip().lower()
        if user_input in ["yes", "no"]:
            return user_input == "yes"
        logging.info("Invalid input. Please enter 'yes' or 'no'.")



def log_state_counts(state_counts_total):
    logging.info("\nTotal records for each state:")
    for state, count in sorted(state_counts_total.items()):
        logging.info(f"{state}: {count}")
    logging.info("\n")


async def main():

    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)

    output_dir = os.path.join(base_dir, "OUTPUT")
    os.makedirs(output_dir, exist_ok=True)

    compress_dir = os.path.join(base_dir, "COMPRESS")
    os.makedirs(compress_dir, exist_ok=True)  

    failed_files_dir = os.path.join(base_dir, "UPLOAD_FAIL_FILES")
    os.makedirs(failed_files_dir, exist_ok=True)

    potential_column_names=  ['Filename', 'Prospect_no', 'LAN_Details', 'CUID_NO']
    lang_folder = os.path.join(base_dir, "lang")
    excel_path = os.path.join(lang_folder, "lang_data.xlsx")
    df = pd.read_excel(excel_path, sheet_name="Sheet1")
    total_records = len(df)
    chunk_size = 500
    total_batches = (total_records + chunk_size - 1) // chunk_size
    batch = 0
    excel_path = os.path.join(lang_folder, "lang_data.xlsx")
    df = pd.read_excel(excel_path, sheet_name="Sheet1")
    bat_file_path = os.path.join(base_dir, "compress.bat") 


    check_and_update_template("BIHAR", ['hindi_template.docx']) 
    check_and_update_template("DELHI", ['hindi_template.docx'])
    check_and_update_template("HARYANA", ['hindi_template.docx'])
    check_and_update_template("JHARKHAND", ['hindi_template.docx'])
    check_and_update_template("HIMACHAL PRADESH", ['hindi_template.docx'])
    check_and_update_template("MADHYA PRADESH", ['hindi_template.docx'])
    check_and_update_template("RAJASTHAN", ['hindi_template.docx'])
    check_and_update_template("CHHATTISGARH", ['hindi_template.docx'])
    check_and_update_template("UTTAR PRADESH", ['hindi_template.docx'])
    check_and_update_template("UTTARAKHAND", ['hindi_template.docx'])

    check_and_update_template("WEST BENGAL", ["bengali_template.docx"])
    check_and_update_template("TRIPURA", ["bengali_template.docx"])

    check_and_update_template("ASSAM", ["assamese_template.docx"])
    check_and_update_template("GUJARAT", ['gujarati_template.docx'])

    check_and_update_template("GOA", ['marathi_template.docx'])
    check_and_update_template("MAHARASHTRA", ['marathi_template.docx'])

    check_and_update_template("PUNJAB", ['punjabi_template.docx'])

    check_and_update_template("TAMIL NADU", ['tamil_template.docx'])

    check_and_update_template("KERALA", ['malayalam_template.docx'])

    check_and_update_template('KARNATAKA', ['kanada_template.docx', 'kannada_template.docx', 'kannda_template.docx'])

    check_and_update_template('ORISSA', ['odiya_template.docx', 'orissa_template.docx', 'orisa_template.docx', 'oriya_template.docx'])
    
    check_and_update_template('ANDHRA PRADESH', ['telugu_template.docx', 'telgue_template.docx', 'telegu_template.docx'])

    check_and_update_template("DEFAULT", ['default_template.docx'])

    log_initial_info(total_records, total_batches, chunk_size)


    running_main_code = get_user_input("Do you want to run the main code? (yes/no): ")
    logging.info(f"Upload status: {running_main_code}")

    user_inputs = {}

    if running_main_code:
        user_inputs['create_new_s3_folder'] = get_user_input("Do you want to create a new AWS S3 folder? (yes/no): ")
        if not user_inputs['create_new_s3_folder']:
            user_inputs['existing_folder_name'] = input("Enter the existing AWS S3 folder name: ").strip()
    else:
        user_inputs['process_dataframe_in_batches'] = get_user_input("Do you want to process dataframe in batches? (yes/no): ")
        user_inputs['check_missing_pdfs'] = get_user_input("Do you want to check missing PDFs? (yes/no): ")
        user_inputs['compressing_pdf'] = get_user_input("Do you want to compress PDFs? (yes/no): ")
        user_inputs['merge_pdfs_by_srno'] = get_user_input("Do you want to merge PDFs by serial number? (yes/no): ")
        user_inputs['create_new_s3_folder'] = get_user_input("Do you want to create a new AWS S3 folder? (yes/no): ")
        if not user_inputs['create_new_s3_folder']:
            user_inputs['want_to_upload_to_S3'] = get_user_input("Do you want to upload to S3? (yes/no): ")
            if user_inputs['want_to_upload_to_S3']:
                user_inputs['existing_folder_name'] = input("Enter the existing AWS S3 folder name: ").strip()
            else:
                user_inputs['existing_folder_name'] = None

    if running_main_code:
        if user_inputs['create_new_s3_folder']:
            user_inputs["make_folder"] = get_user_input("Do you want to use the default folder or create a new one? (yes/no): ")
            if not user_inputs['make_folder']:
                user_inputs['folder_name'] = input("Enter the folder name: ").strip()
                if user_inputs['folder_name']:
                    S3_URI = create_s3_folder(user_inputs['folder_name'])
                    logging.info(f"Creating new folder: {S3_URI}")
                else:
                    logging.warning("Folder name cannot be empty. Defaulting to creating in the root of S3 bucket.")
                    S3_URI = create_s3_folder(S3_BUCKET_NAME)
                    logging.info(f"Creating new folder: {S3_URI}")
            else:
                S3_URI = create_s3_folder(S3_BUCKET_NAME)
                logging.info(f"Creating new folder: {S3_URI}")
        else:
            S3_URI = user_inputs['existing_folder_name']
            logging.info(f"Uploading to existing folder: {S3_URI}")

        state_counts_total = df['State'].value_counts().to_dict()
        log_state_counts(state_counts_total)

        process_dataframe_in_batches(df, chunk_size, total_batches, batch, output_dir, state_template_dict, potential_column_names)

        logging.info("Checking All PDFs...")
        missing_pdfs_list = check_missing_pdfs(lang_folder, output_dir, potential_column_names)
        if missing_pdfs_list:
            print("Missing PDFs List:")
            for missing_pdf in missing_pdfs_list:
                print(f"- {missing_pdf}")
            process_missing_pdfs(lang_folder, output_dir, missing_pdfs_list, state_template_dict)

        logging.info("All PDFs files are created successfully!\n")
        compressing_pdf(output_dir, bat_file_path, compress_dir)
        await delete_directory(output_dir)
        uploading_to_s3(compress_dir, S3_URI, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, failed_files_dir)
    else:
        state_counts_total = df['State'].value_counts().to_dict()
        log_state_counts(state_counts_total)

        if user_inputs['process_dataframe_in_batches']:
            process_dataframe_in_batches(df, chunk_size, total_batches, batch, output_dir, state_template_dict, potential_column_names)

        logging.info("Checking All PDFs...")
        if user_inputs['check_missing_pdfs']:
            missing_pdfs_list = check_missing_pdfs(lang_folder, output_dir, potential_column_names)
            if missing_pdfs_list:
                print("Missing PDFs List:")
                for missing_pdf in missing_pdfs_list:
                    print(f"- {missing_pdf}")
                process_missing_pdfs(lang_folder, output_dir, missing_pdfs_list, state_template_dict)

        logging.info("All PDFs files are created successfully!\n")

        if user_inputs['compressing_pdf']:
            compressing_pdf(output_dir, bat_file_path, compress_dir)
            await delete_directory(output_dir)

        if user_inputs['merge_pdfs_by_srno']:
            logging.info("Processing Merge PDFs")
            merge_pdfs_by_srno(compress_dir, base_dir, excel_path, potential_column_names, batch_size=2000)
            logging.info("All pdfs are merged successfully!\n")

        if user_inputs['create_new_s3_folder']:
            user_inputs["make_folder"] = get_user_input("Do you want to use the default folder or create a new one? (yes/no): ")
            if not user_inputs['make_folder']:
                user_inputs['folder_name'] = input("Enter the folder name: ").strip()
                if user_inputs['folder_name']:
                    S3_URI = create_s3_folder(user_inputs['folder_name'])
                    logging.info(f"Creating new folder: {S3_URI}")
                else:
                    S3_URI = create_s3_folder(S3_BUCKET_NAME)
                    logging.info(f"Creating new folder: {S3_URI}")
            else:
                S3_URI = create_s3_folder(S3_BUCKET_NAME)

                logging.info(f"Creating new folder: {S3_URI}")
        else:
            if user_inputs['existing_folder_name']:
                S3_URI = user_inputs['existing_folder_name']
                logging.info(f"Uploading to existing folder: {S3_URI}")
            else:
                logging.warning("No existing folder name provided. Skipping S3 upload.")
                S3_URI = None

        if S3_URI:
            uploading_to_s3(compress_dir, S3_URI, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, failed_files_dir)

if __name__ == '__main__':
    asyncio.run(main())


