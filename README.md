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
7. Catalogued the files(Data Catalog) that would be entering in the S3 Bucket, so I could query them in Athena by building a table on top of them if needed 

The 1st run of the AWS Glue Pipeline saw the successful loading of data in the form of 'parquet' files in the S3 bucket. The query in Athena confirmed that all our records(68883) had loaded successfully in the S3 Bucket

![Athena_query1](https://github.com/user-attachments/assets/48e23d0d-73e9-4a0f-9c5a-efdee255eecc)


I now inserted a single record and ran the Glue Pipeline again. The query on Athena proved that this record too had got loaded in s3 successfully. The query in Athena proved I now had 68884 records

![Athena_query](https://github.com/user-attachments/assets/b2e5a8aa-1ffb-4790-9e36-f3601ef3d5d7)


I went for another run of my Glue Pipeline. Post this run when I queried in Athena I found that there were 137767 records(68883+1+68883). This meant that my pipeline was reinserting the already inserted records in S3 which causes a duplication of Records. Hence I would need to do an incremental processing as an enhancement in my pipeline



## Iteration 2

In a bid to now do an incremental processing, I decide to introduce a 'Metadata' Table which would keep track of the table and the the order_time of the record which was last loaded on to the S3 Bucket.

Let us say my orders table looks like this 

![image](https://github.com/user-attachments/assets/2a66369f-1c1d-456c-a247-ec54d74254a8)

I now create a metadata table called 'fetch_details' using

CREATE TABLE fetch_details(
metadata_id INT AUTO_INCREMENT PRIMARY KEY,
tablename VARCHAR(255) NOT NULL,
last_fetched DATETIME NOT NULL,
updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

and I just insert a record in the fetch_details' table 

insert into fetch_details (tablename, last_fetched) values('orders','1900-01-01 00:00:00');

My fetch_details table would look like

![image](https://github.com/user-attachments/assets/17e6a18d-eec1-4555-9e3b-41eb5db23b28)

The last_fetched column in this table shows a '1900-01-01 00:00:00' which is a random date. My intention is to refer the 'fetch_details' table and pick the last entry in the 'last_fetched' column . Then go to the 'orders' table and pick all entries which have an 'order_last_updated' date greater than the the last entry in the 'last_fetched' column of the 'fetch_details' table. These records will then be subjected to the Glue Pipeline and only entries after a certain datetime will be loaded on to the S3 bucket. 

At this stage, when I run my Glue Pipeline, all 10 records in my 'orders' table will be transferred to the S3 bucket, since all the entries have 'order_last_updated' date after the last entry in the 'last_fetched' column of the 'fetch_details' table.

Meanwhile, we will also update the metadata table as:- 

insert into fetch_details (tablename, last_fetched) values('orders','2024-07-31 17:50:18');


![metadata extraction](https://github.com/user-attachments/assets/54a6c811-c9da-4e11-ac6a-1194e3b58e51)

Once the 10 entries of the 'orders' table have been transferred to the 'S3 Bucket' the last or the max date of the 'order_last_updated' column of the 'orders' table will be updated in the 'fetch_details' table.

This means that if we were to run the Glue Pipeline again, then only entries after '2024-07-31 17:50:18' will be processed and loaded onto the S3 bucket.

Once this concept was established, I re-did my pipeline as follows:

![ETL](https://github.com/user-attachments/assets/0032316d-804e-4106-9198-f0e23269aaf2)

I have used the 'SQL Query' Transformation to pull the last date in the 'fetch_details' table and use it as a fileter to pick only those records from the 'orders' table after that data.

The query I used here is as follows:

select * from orders where order_last_updated > = (select max(last_fetched) from fetch_details where tablename = 'orders')

Our Pipeline is now capable to do incremental processing. However even at this stage I am still manually updateing my metadata table and self - triggering my pipeline. There is more scope for improvement.

## Iteration 3

To automate the entry in the fetch_details(metadata) table, we wont find many solutions in the 'Visual ETL' method, hence it is better to shift to the 'Script' Method. The moment we do that our 'Visual ETL' can be seen in code format.

![Code](https://github.com/piperalpha7/AWS_Pipeline_RDS_to_S3_Project/blob/main/glue_CD_script.py)

Through the 'Script' mode we have now succesfully created a pipeline 'Pipeline1' which extracts, transforms and loads incremental data in AWS 'S3' bucket. We will schedule a trigger for this pipeline later


## Iteration 4

Next, I want to build a 'Data Quality' Pipeline(Pipeline2) wherein I want to only keep the valid 'order_status' and want to discard the rest 

So, first I creat 3 more folders in my S3 bucket viz. 'Staging', 'Discarded' and 'Archived'.

So, once my data lands in the 'Landing folder of my S3 bucket, I would carry on my Data Quality check:-
1. Records which pass the check will move to the 'Staging' folder.
2. Records which DO NOT pass the check will move to the 'Discarded' folder.
3. After the records in the 'Landing' folder go through the data quality checks, they will all go to the 'archived' folder' thereby vacating the 'landing' folder.

 Firstly, I create a new my SQL Table:-

 CREATE TABLE order_status_lookup (
    status_name VARCHAR(50) NOT NULL);

    INSERT INTO order_status_lookup (status_name) 
VALUES 
('ON_HOLD'),
('PAYMENT_REVIEW'),
('PROCESSING'),
('CLOSED'),
('SUSPECTED_FRAUD'),
('COMPLETE'),
('PENDING'),
('CANCELED'),
('PENDING_PAYMENT');


The table will have all the valid Order Status and will look like:-

![image](https://github.com/user-attachments/assets/9c2185d0-1745-4613-90f7-d5d654eb99ac)

The pipeline will look like:-


![pipeline2](https://github.com/user-attachments/assets/09ace338-3b85-47ae-bad6-ae52ce83af13)


Let me explain the components of my Pipeline2. They are as follows:

Left Join(Transform):
I have used this Transformation to see which order statuses in the orders table are invalid. The valid order statuses as described above are in 'order_status_lookup' table. 

![image](https://github.com/user-attachments/assets/8963fbff-f097-4a95-acb1-5a6253294794)

You see in the above image due to left join the 'order_status' that are invalid are earmarked by the adjoining null values in the 'status_name' Column...(please note the 'order_status' column is from the 'orders' table and the 'status_name' column is from the 'order_status_lookup' table).

SQL Query(Transform):

Once the Invalid statuses are identified we substitute the null values in the'status_name' column with the keyword 'Invalid'.

This is donw with the help of the following query:-

select order_id,order_last_updated,customer_id,order_status, coalesce(status_name,'Invalid') from cte

The joined result will now look like:- 

![image](https://github.com/user-attachments/assets/ae843efa-8bad-446d-a1e9-0b04350357f0)



Conditional Router(Transform) - 

We use the 'Conditional' Router Transform and classify all the records with 'Invalid'  as 'status_name' to get diverted to 'Incorrect Records'and the rest as 'Correct Records'

The correct records will then got the 'Staging' folder as described above while the Incorrect recors go the 'Discarded' Folder.

All the Records post quality check needed to go to the 'Archived' folder while vacating the 'Staging' folder, which wasnt possible through Visual ETL, hence here I shifted to the 'Script' mode and modified the code. The 'boto3' library which is used to manipulate objects in S£ was very handy here. The code is attached :

![Code]https://github.com/piperalpha7/AWS_Pipeline_RDS_to_S3_Project/blob/main/Pipeline2.py








