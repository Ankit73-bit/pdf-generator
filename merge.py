import PyPDF2
import os

def merge_pdfs(input_folder, output_path):
    pdf_merger = PyPDF2.PdfMerger()

    for item in os.listdir(input_folder):
        if item.endswith('.pdf'):
            pdf_path = os.path.join(input_folder, item)
            pdf_merger.append(pdf_path)

    with open(output_path, 'wb') as output_pdf:
        pdf_merger.write(output_pdf)

if __name__ == "__main__":
    input_folder = 'INPUT'  # Replace with the path to your folder containing PDFs
    output_path = 'OUTPUT/Bill No. 72.pdf'  # Replace with the path for the merged PDF

    merge_pdfs(input_folder, output_path)
    print(f"PDFs merged successfully into {output_path}")
