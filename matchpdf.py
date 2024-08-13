import os
import pandas as pd
import shutil

def read_barcodes_from_excel(excel_path, sheet_name, barcode_column):
    """Reads barcodes from an Excel file."""
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    barcodes = df[barcode_column].astype(str).tolist()
    return barcodes

def list_pdf_files(directory):
    """Lists all PDF files in the specified directory."""
    return [f for f in os.listdir(directory) if f.endswith('.pdf')]

def check_barcodes_in_pdfs(barcodes, pdf_files):
    """Checks if barcodes match any PDF file names."""
    matched_barcodes = []
    matched_pdf_files = []
    for barcode in barcodes:
        for pdf in pdf_files:
            if barcode in pdf:
                matched_barcodes.append(barcode)
                matched_pdf_files.append(pdf)
                break
    return matched_barcodes, matched_pdf_files

def copy_matched_pdfs(pdf_files, source_directory, target_directory):
    """Copies matched PDF files to a new directory."""
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    for pdf in pdf_files:
        source_path = os.path.join(source_directory, pdf)
        target_path = os.path.join(target_directory, pdf)
        shutil.copyfile(source_path, target_path)

def main():
    excel_path = 'path_to_your_excel_file.xlsx'
    sheet_name = 'Sheet1'  # Change this if your sheet name is different
    barcode_column = 'Barcode'  # Change this to your barcode column name
    pdf_directory = 'path_to_your_pdf_directory'
    target_directory = 'path_to_your_target_directory'

    barcodes = read_barcodes_from_excel(excel_path, sheet_name, barcode_column)
    pdf_files = list_pdf_files(pdf_directory)
    matched_barcodes, matched_pdf_files = check_barcodes_in_pdfs(barcodes, pdf_files)

    print("Barcodes matched with PDF files:")
    for barcode in matched_barcodes:
        print(barcode)

    copy_matched_pdfs(matched_pdf_files, pdf_directory, target_directory)
    print(f"Copied {len(matched_pdf_files)} PDF files to {target_directory}")

if __name__ == "__main__":
    main()
