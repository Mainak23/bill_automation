import os
import shutil
import pdfplumber
import datetime
from cryptography.fernet import Fernet
from PyPDF2 import PdfReader, PdfWriter,PdfMerger
import chromadb
from abc import ABC, abstractmethod
import re

class FolderOperation(ABC):
    def __init__(self):
        # Set paths once in the constructor to avoid redundancy
        self.parent_folder = os.path.join(os.getcwd(), "parent_folder")
        self.split_pdf_folder = os.path.join(self.parent_folder, "split_pdf_folder")
        self.output_folder = os.path.join(os.getcwd(), "out_folder")
        self.db_path = os.path.join(os.getcwd(), "data_base")
        self.collection_name = "collection"

        # Folder creation list
        self.folder_create = [
            self.parent_folder, self.split_pdf_folder, 
            self.output_folder, self.db_path
        ]

        # Folder to delete
        self.delete_folder = self.parent_folder

    @abstractmethod
    def create_folder(self):
        """Creates all necessary folders."""
        for folder in self.folder_create:
            os.makedirs(folder, exist_ok=True)
        print("All folders created successfully!")

    @abstractmethod
    def delete_files_in_directory(self):
        """Deletes all files in a directory."""
        try:
            for root, _, files in os.walk(self.delete_folder):
                for file in files:
                    os.remove(os.path.join(root, file))
            print(f"All files deleted from {self.delete_folder}")
        except Exception as e:
            print(f"Error deleting files: {e}")


class ExtractText(FolderOperation):
    def __init__(self, pdf_path):
        super().__init__()  # Initialize parent class
        self.pdf_path = pdf_path
        self.key = Fernet.generate_key()
        self.cipher = Fernet(self.key)

    def create_folder(self):
        """Override create_folder in the child class if needed"""
        # In this case, we are using the parent class method
        return super().create_folder()

    def split_pdf(self):
        with open(self.pdf_path, 'rb') as fo:
            pdf_reader = PdfReader(fo)
            if len(pdf_reader.pages) > 0:
                for page_number, page in enumerate(pdf_reader.pages):
                    pdf_writer = PdfWriter()
                    pdf_writer.add_page(page)
                    output_pdf_path = f"{self.split_pdf_folder}/page_{page_number}.pdf"
                    with open(output_pdf_path, 'wb') as output_file:
                        pdf_writer.write(output_file)
            else:
                shutil.move(self.pdf_path, self.split_pdf_folder)


    def split_pdf_specific_page(self,page_number):
        """Splits the PDF into separate pages and stores them."""
        with open(self.pdf_path, 'rb') as fo:
            # Initialize PDF reader and writer
            pdf_reader = PdfReader(fo)
            writer = PdfWriter()
            writer.add_page(pdf_reader.pages[page_number])
                # Define the output path for each split page
            output_pdf_path = f"{self.split_pdf_folder}/page_{page_number}.pdf"
                # Write the page to the output file
            with open(output_pdf_path, 'wb') as out_pdf:
                writer.write(out_pdf)
            print(f"Page {page_number} saved to {output_pdf_path}")
            return output_pdf_path


    def label_and_extract_text(self):
        """Extract text from PDF and associate it with page names."""
        extracted_text = {}
        with pdfplumber.open(self.pdf_path) as pdf:
            for idx, page in enumerate(pdf.pages):
                extracted_text[f"page_{idx}.pdf"] = page.extract_text()
        return extracted_text

    def extract_pdf(self, file_name):
        """Copy and encrypt the file after extraction."""
        target_pdf_path = os.path.join(self.split_pdf_folder, file_name)
        creation_time = os.path.getctime(target_pdf_path)
        creation_time_readable = datetime.datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d_%H-%M-%S')
        encrypted_name = self.cipher.encrypt(f"{file_name}_{creation_time_readable}".encode())
        new_file=f"{encrypted_name[16:20].decode()}.pdf"
        target_path = os.path.join(self.output_folder, new_file)  # Store using encrypted name
        shutil.copy(target_pdf_path, target_path)
        print(f"Copied and encrypted {file_name} to {target_path}")
        return target_path

    def delete_files_in_directory(self):
        # In this case, we are using the parent class method
        return super().delete_files_in_directory()


class localvectordatabase():
    def __init__(self):
        self.db_path = os.path.join(os.getcwd(), "data_base")
        self.collection_name="collection"
        self.client=chromadb.PersistentClient(path=self.db_path)
        self.collection=self.client.get_or_create_collection(name=self.collection_name)

    def local_data_base(self,text_map:str) ->str:
        for key,value in text_map.items():
            self.collection.add(
                    documents=value,
                    ids=key
                    )
            print(f"Added documents to the collection.")

    def documen_quary(self,quarry:str)->str:
        results_all = self.collection.query(query_texts=[" "],where_document={"$contains": quarry})
        value_=results_all ["documents"]
        ids= results_all["ids"][0]
        #print(ids)
        return ids,value_
    
    def delete_colection(self):
        self.client.delete_collection(self.collection_name)
    
# Example usage:
# 1. Folder creation and file operations

# 2. Extracting text from a PDF

class scherch():
    def __init__(self,path,quary_primary,quary_secondary):
        self.path=path
        self.quary=quary_primary
        self.quary_1=quary_secondary
        self.indivisual_path=self.scerch_1()
        pass
#"Registration Certificate"
    def scerch_1(self):
        pdf_extractor = ExtractText(pdf_path=self.path)
        pdf_extractor. create_folder() 
        extracted_text = pdf_extractor.label_and_extract_text()
        #print(extracted_text)
        #pdf_extractor.split_pdf()
        # 3. Adding extracted text to the database
        local_db = localvectordatabase()
        local_db.local_data_base(extracted_text)
        id , _ = local_db.documen_quary(self.quary)
        local_db.delete_colection()
        page_number=int(id[0].split(".")[0].split("_")[1])
        #print(page_number)
        pdf_extractor.split_pdf_specific_page(page_number)
        path_=pdf_extractor.extract_pdf(id[0])
        pdf_extractor.delete_files_in_directory()
        return path_
    
    def next_scerch(self):
        pdf = pdfplumber.open(self.indivisual_path)
        page = pdf.pages[0]
        print(len(page.extract_table()))
        output = {}
        count=0
        for row in page.extract_table():
            # Filter out None values and join the non-None items with newline
            filtered_row = '\n'.join(str(item) for item in row if item is not None)
            count=count+1
            output[f'{count}']=filtered_row
        # Join all rows with two newlines between them to format them nicely
        print(output)
        local_db_1 = localvectordatabase()
        local_db_1.local_data_base(output)
        id_1 , values = local_db_1.documen_quary(self.quary_1)
        local_db_1.delete_colection()
        #page_number=int(id_1[0])
        #print(values)
        array_1d = [item for sublist in values for item in sublist]
        value="\n".join(element for element in array_1d)
        return value


if __name__ == "__main__":
    path_1=r"C:\Users\USER\Desktop\paid_ocr\Arodek GST Reg-6.pdf"
    final_=scherch(path_1,"Registration Certificate","Add")

    print(final_.next_scerch())


