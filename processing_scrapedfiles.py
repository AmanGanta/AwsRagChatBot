import boto3
import os
import re
from langchain_community.chat_models import ChatOpenAI
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("FIRECRAWL_API_KEY")

# Function to get data from S3 and clean it
def get_and_clean_data_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    raw_data = response['Body'].read().decode('utf-8', errors='ignore')

    # Clean the text: remove empty lines and keep only characters and numbers
    def clean_text(text):
        lines = text.splitlines()
        non_empty_lines = [line for line in lines if line.strip()]
        cleaned_text = "\n".join(non_empty_lines)
        cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned_text)
        return cleaned_text

    cleaned_data = clean_text(raw_data)
    return cleaned_data

# Function to format text using GPT-4 in LangChain
def format_text_with_llm(text, max_chunk_length=8000):
    llm = ChatOpenAI(model="gpt-4")
    formatted_chunks = []

    # Split text into chunks that fit within the context limit
    while len(text) > max_chunk_length:
        split_point = text.rfind(' ', 0, max_chunk_length)
        if split_point == -1:
            split_point = max_chunk_length
        chunk = text[:split_point]
        text = text[split_point:].strip()
        formatted_chunks.append(llm.invoke(f"Please format the following scraped file into a good looking proper text:\n\n{chunk}"))

    if text:
        formatted_chunks.append(llm.invoke(f"Please format the following scraped file into a good looking proper text:\n\n{text}"))

    formatted_text = "\n\n".join([chunk.content for chunk in formatted_chunks])
    return formatted_text

# Function to save the formatted text to a new local file
def save_to_file(content, file_name):
    with open(file_name, 'w') as file:
        file.write(content)
    print(f"Formatted content saved to {file_name}")

# Function to upload the formatted file back to S3
def upload_local_file_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading local file: {e}")

def main():
    bucket_name = 'ragproject-55612'
    original_file_key = 'policy_documents/rental_policy_text.txt'
    processed_file_key = 'policy_documents/processed-text-webfile.txt'
    
    # Step 1: Access and clean the data from S3
    data = get_and_clean_data_from_s3(bucket_name, original_file_key)
    print("Data from S3 loaded and cleaned successfully.")
    
    # Step 2: Format the text using GPT-4 in LangChain
    formatted_text = format_text_with_llm(data)
    print("Text formatted successfully.")
    
    # Step 3: Save the formatted text to a local file
    output_file = 'formatted_policy_text.txt'
    save_to_file(formatted_text, output_file)
    
    # Step 4: Upload the formatted file back to S3
    upload_local_file_to_s3(output_file, bucket_name, processed_file_key)
    print(f"Formatted text uploaded to s3://{bucket_name}/{processed_file_key}")

    # Optional: Remove the local file if needed
    os.remove(output_file)
    print(f"Temporary file {output_file} removed.")

if __name__ == "__main__":
    main()
