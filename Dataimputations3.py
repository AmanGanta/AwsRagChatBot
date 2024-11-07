import boto3
import os
from langchain_community.document_loaders import FireCrawlLoader
from dotenv import load_dotenv
load_dotenv()

api_key = os.getenv("FIRECRAWL_API_KEY")
def create_s3_bucket(bucket_name, region=None):
    s3 = boto3.client('s3', region_name=region)
    try:
        if region is None:
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"Bucket '{bucket_name}' created successfully.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket '{bucket_name}' already exists and is owned by you.")
    except Exception as e:
        print(f"Error creating bucket: {e}")

def upload_local_file_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading local file: {e}")

def crawl_and_save_to_file(urls, file_name, depth=1, max_pages=20):
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        raise ValueError("FIRECRAWL_API_KEY not set in environment variables.")
    
    with open(file_name, "w") as file:
        for url in urls:
            print(f"Processing URL: {url}")
            loader = FireCrawlLoader(
                api_key=api_key,
                url=url,
                mode="scrape"
            )

            try:
                docs = loader.load()
                for doc in docs:
                    file.write(str(doc) + "\n")
            except Exception as e:
                print(f"Error while processing {url}: {e}")
    
    print(f"Crawled content saved to {file_name}")
    return file_name

def main():
    bucket_name = "ragproject-55612"
    local_file_path = "Lease policy.docx"  
    web_file_name = "rental_policy_text.txt"  
    create_s3_bucket(bucket_name=bucket_name,region="us-west-2")
    s3_key_policy = "policy_documents/policy_document.pdf"
    upload_local_file_to_s3(local_file_path, bucket_name, s3_key_policy)
    url =["https://www.apartments.com/amherst-manor-apartments-williamsville-ny/6qjx6qv/","https://www.zillow.com/apartments/williamsville-ny/amherst-manor-apartments/5j8y5p/","https://www.rentcafe.com/apartments/ny/amherst/amherst-manor-apartments/default.aspx"] 
    temp_file_path = crawl_and_save_to_file(url, web_file_name, depth=1, max_pages=20)

    s3_key_web_data = "policy_documents/rental_policy_text.txt"
    upload_local_file_to_s3(temp_file_path, bucket_name, s3_key_web_data)

    os.remove(temp_file_path)
    print(f"Temporary file {temp_file_path} removed.")

if __name__ == "__main__":
    main()