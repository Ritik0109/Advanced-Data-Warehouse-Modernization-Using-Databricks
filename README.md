# Advanced-Data-Warehouse-Modernization-Using-Databricks

### About
FinanceCo Global is a financial services firm supporting regulatory reporting and enterprise analytics. To support growth and compliance needs, the company required a modern, scalable data platform to replace its legacy on-premises warehouse.

### Challenge
The existing SQL Server–based warehouse relied on complex SSIS workflows and manual Databricks deployments, leading to poor scalability, slow month-end reporting, high maintenance costs, and weak governance over sensitive financial data.

### Solution
I implemented an Azure Databricks Lakehouse using a Bronze–Silver–Gold architecture. Legacy ETL processes were migrated to scalable ELT pipelines with Delta Lake, SCD Type 1 modeling, and performance optimizations such as Z-Ordering and caching along with Unity Catalog provided centralized governance.

### Details
Data is ingested via Azure Data Factory into ADLS and processed in Databricks using PySpark. Delta tables power high-performance analytics, fine-grained access control, and full data lineage, delivering a secure, scalable, and compliant financial analytics platform.

 Data Lineage             |  Pipeline
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/fb8f735f-9f62-4487-a8b2-5a5604b63557" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/979d1b3d-f7a4-42f9-8763-530c021dd6d5" />

 Access Control             |  Custom Function
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/1f88d33e-8a0c-49fb-984b-56d85d10d625" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/4fbd1ebe-7fc4-43a3-8aba-50d498bdc2d6" />

 Row-Level-Security             |  Column Level Masking
:-------------------------:|:-------------------------:
<img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/5e8cf392-cda9-4a41-854f-b84fecdb6348" />  |  <img width="480" height="270" alt="image" src="https://github.com/user-attachments/assets/bf53923e-7ae4-4785-9736-4fe49f5c514b" />





