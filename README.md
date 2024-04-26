<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-14.3ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/14.3lts-ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)

## Challenges Addressed
The Internet of Things (IoT) is generating an unprecedented amount of data, with IBM estimating that annual processing will reach approximately 175 zettabytes by 2025. The true value of IoT data lies in its real-time processing, enabling businesses to make timely, data-driven decisions. However, the diverse and ever-changing nature of IoT data formats and schemas poses significant challenges for many organizations. At Databricks, we recognize these obstacles and provide a comprehensive data intelligence platform to help manufacturing organizations effectively process and analyze IoT data, unlocking its full potential. By leveraging Databricks' platform, manufacturing organizations can transform their IoT data into actionable insights, driving efficiency, reducing downtime, and improving overall operational performance. In this blog, we share an example of how Databricks can create efficiencies in your business.

In the manufacturing industry, managing and analyzing time series data at scale and in real-time can be a significant challenge. One such complex task is calculating an exponential moving average in parallel across a cluster. However, Databricks has introduced Delta Live Tables (DLT) as a fully managed ETL solution, simplifying the operation of time series pipelines and reducing the complexity involved. DLT offers features such as schema inference and data quality enforcement, ensuring that data quality issues are identified without allowing schema changes from data producers to disrupt the pipelines. By integrating Databricks Labs' open-source Tempo library for time series analysis, Databricks provides a user-friendly interface for parallel computation of complex time series operations, including exponential weighted moving averages, interpolation, and resampling. Moreover, Databricks enables stakeholders to be notified of anomalies in real-time by feeding the results of our streaming pipeline into SQL alerts. Finally, with Lakeview Dashboards, manufacturing organizations can gain valuable insights into how metrics, such as defect rates by factory, might be impacting their bottom line. Databricks' innovative solutions help manufacturing organizations overcome their data processing challenges, enabling them to make informed decisions and optimize their operations.

To get started with this Solution Accelerator, run the setup notebook and use the DLT pipeline it creates.

## Reference Architecture
<img src='https://raw.githubusercontent.com/databricks-industry-solutions/iot-distributed-ml/master/images/reference_architecture.png' width=800>

## Authors
josh.melton@databricks.com

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

## License

&copy; 2024 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].