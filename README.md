# AWS_Pipeline_RDS_to_S3_Project
Extract Load Transform(ETL) Pipeline from AWS RDS instance to AWS S3 Bucket

In this project , I created a data pipeline in AWS Glue which ingested Data from an AWS RDS instance and loaded it into an AWS S3 bucket. Carried out some transformations in between and used triggers to automate the Pipeline. This pipeline was built Iteratively as follows:

## Iteration 1

1. Created an AWS RDS Instance(MYSQL)

   ![image](https://github.com/user-attachments/assets/929f9efe-6bcd-4de3-8591-9d297d63e36e)



\
\
2. Connected MySQL Workbench to the AWS RDS Instance

![image](https://github.com/user-attachments/assets/7a9f9e45-8705-451f-aedd-6a34032bda6a)


\
\
3. Created Orders Table in AWS RDS instance through MySQL Workbench



   CREATE TABLE orders (
    order_id INT,              
    order_last_updated DATETIME,          
    customer_id INT,               
    order_status VARCHAR(25)
);

\
\
4. Inserted records in this Table through 'LOAD LOCAL INFILE COMMAND'


   EG. LOAD DATA INFILE 'data.txt' INTO TABLE db2.my_table;

   Inserted 68883 records

\
\
5. Create an AWS S3 Bucket(eg.du-aws-project) and then a folder called 'Orders' . Inside the 'Orders' Folder I created another Folder called 'Landing'.

\
\
6. In AWS Glue I created a Visual ETL

![Pipeline1_1](https://github.com/user-attachments/assets/5706ba2e-a273-49cd-bcf0-05a0e9f0e7a0)

Source - RDS
Transform - Change Schema ( Adjusting Datatypes)
Target - AWS S3 Bucket('Landing' Folder) 

\
\
7. Catalogued the files(Data Catalog) that would be entering in the S£ Bucket, so I could query them in Athena by building a table on top of them if needed 

The 1st run of the AWS Glue Pipeline saw the successful loading of data in the form of 'parquet' files in the S3 bucket. The query in Athena confirmed that all our records(68883) had loaded successfully in the S3 Bucket

![Athena_query1](https://github.com/user-attachments/assets/48e23d0d-73e9-4a0f-9c5a-efdee255eecc)


I now inserted a single record and ran the Glue Pipeline again. The query on Athena proved that this record too had got loaded in s3 successfully. The query in Athena proved I now had 68884 records

![Athena_query](https://github.com/user-attachments/assets/b2e5a8aa-1ffb-4790-9e36-f3601ef3d5d7)


I went for another run of my Glue Pipeline. Post this run when I queried in Athena I found that there were 137767 records(68883+1+68883). This meant that my pipeline was reinserting the already inserted records in S3 which causes a duplication of Records. Hence I would need to do an incremental processing as an enhancement in my pipeline


\
\
## Iteration 2




