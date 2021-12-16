import pandas as pd   
import boto3 

def main():
    #lets us initialize our s3 resource
    s3 = boto3.client('s3')
    bucket_name = 'blossom-data-eng-richmond'

    #Downloading file from the s3 bucket.
    s3.dowload_file('blossom-data-engs', 'free-7-million-company-dataset.zip','companies-sorted.zip')

    #Loading our datast into a dataframe
    comp_data = pd.read_csv("companies_sorted.csv", index_col = 0, compression = 'zip')

    # Checkong the shape of our Dataframe
    comp_data.shape

    #Cheking out the number companies that do not hacve domain names.
    empty_comp = comp_data['domain'].isna().values.flattern.sum()

    #Print total number of companies that do not have domain names.
    print(empty_comp)

    # Time To droom all companies that do not have domain names from our datset
    comp_data.dropna(subset=['domain'], inplace = True)
    print(comp_data.shape)

    # After dropping irrelevant rows, we write output to a file in JSON format.
    comp_data.to_json('free-7-million-company-datast-json.gzip',compression = 'gzip')
    
    #Now writing same file but this time in parquet format.
    comp_data.to_parquet('free-7-million-company_dataset.parquet')

    # Uploading files to bucket
    filename1 = 'free-7-million-company-datast-json.gzip'
    filename2 = 'free-7-million-company-datast.parquet'

    s3.upload_file(filename1, bucket_name, filename1) # Upload JSON file
    s3.upload_file(filename2,bucket_name, filename2) # Upload parquet filen


if __name__ =='__main__':
    main()