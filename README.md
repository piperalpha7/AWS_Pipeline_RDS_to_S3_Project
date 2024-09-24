# AWS_Pipeline_RDS_to_S3_Project
Extract Load Transform(ETL) Pipeline from AWS RDS instance to AWS S3 Bucket

In this project , I created a data pipeline in AWS Glue which ingested Data from an AWS RDS instance and loaded it into an AWS S3 bucket. Carried out some transformations in between and used triggers to automate the Pipeline. This pipeline was built Iteratively as follows:

## Iteration 1

1. Created an AWS RDS Instance(MYSQL)

   ![image](https://github.com/user-attachments/assets/929f9efe-6bcd-4de3-8591-9d297d63e36e)




2. Connected MySQL Workbench to the AWS RDS Instance

![image](https://github.com/user-attachments/assets/7a9f9e45-8705-451f-aedd-6a34032bda6a)




3. Created Orders Table in AWS RDS instance through MySQL Workbench



   CREATE TABLE orders (
    order_id INT,              
    order_last_updated DATETIME,          
    customer_id INT,               
    order_status VARCHAR(25)
);



4. Inserted records in this Table through 'LOAD LOCAL INFILE COMMAND'


   EG. LOAD DATA INFILE 'data.txt' INTO TABLE db2.my_table;


