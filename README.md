# Immigration Data Lake <!-- omit in toc -->

The scope of the project is to build a data lake on S3. The data lake will allow users to query immigration data. Moreover, the data lake will provide weather, demographic and airport data for immigration.

## 1. Table of Contents

- [1. Table of Contents](#1-table-of-contents)
- [2. Project Scope](#2-project-scope)
- [Project Structure](#project-structure)
- [3. Datasets](#3-datasets)
- [4. Data Source Description](#4-data-source-description)
  - [4.1. I94 Immigration Data](#41-i94-immigration-data)
  - [4.2. World Temperature Data](#42-world-temperature-data)
  - [4.3. U.S. City Demographic Data](#43-us-city-demographic-data)
- [5. Data Exploration](#5-data-exploration)
  - [5.1. Immigration Dataset](#51-immigration-dataset)
  - [Demographics Data](#demographics-data)
  - [5.2. World Temperature Data](#52-world-temperature-data)
- [Data Model](#data-model)
  - [Purpose of data model](#purpose-of-data-model)
  - [Schema](#schema)
  - [Immigration Facts Table](#immigration-facts-table)
  - [Person Dimension Table](#person-dimension-table)
  - [Demographics Dimension Table](#demographics-dimension-table)
  - [Weather Dimension Table](#weather-dimension-table)
- [Data Quality Checks](#data-quality-checks)
- [Project Setup](#project-setup)
  - [Setup environment](#setup-environment)
  - [AWS CLI](#aws-cli)
  - [AWS Configure](#aws-configure)
    - [Generate Credentials](#generate-credentials)
    - [Configure AWS for AWS CLI](#configure-aws-for-aws-cli)
  - [Create EMR Roles](#create-emr-roles)
  - [6.1. Setup Docker and Docker-Compose](#61-setup-docker-and-docker-compose)
  - [6.3. Pyspark](#63-pyspark)
  - [Setup AWS](#setup-aws)
  - [Airflow](#airflow)
  - [Reminder](#reminder)
- [References](#references)

## 2. Project Scope

In the following sections we will first start by describing the datasets that we have. Afterwards we will gather the data and provide the source of every dataset. We will then do some data exploration before defining the data model. Once the data model has been provided, we will design the ELT pipeline. Scripts and instructions will be provided to upload the data into an S3 bucket and execute the pipeline on AWS.

## Project Structure

The data exploration is done in data_exploration.ipynb and uses [pyspark docker](https://hub.docker.com/r/jupyter/pyspark-notebook) [1].

The ETL pipeline uses a local airflow instance. The dags are defined locally and are based on this [tutorial](https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/) [2].

The airflow docker used is developed by puckel and is obtained from this [repository](https://github.com/puckel/docker-airflow) [3]. The LICENSE file is included in this repository under the name puckel_docker_airflow_license.txt.

## 3. Datasets

- I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.
- World Temperature Data: This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
- U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

## 4. Data Source Description

### 4.1. I94 Immigration Data

Each report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).

### 4.2. World Temperature Data

This dataset contains 5 variables: the date, the average temperature, the average temperature uncertainty, the state and the country.

### 4.3. U.S. City Demographic Data

This dataset contains 12 variables: city, state, race, count, median age, male population, female population, total population, number of veterans, foreign-born, average household size and state code.

## 5. Data Exploration

Our data exploration was done in our data_exploration.ipynb notebook.

### 5.1. Immigration Dataset

The dictionnary is obtained from the I94_SAS_Labels_Description.SAS file provided with the I-94 dataset.

| Column Name | Description |
| :--- | :--- |
| CICID* | ID |
| I94YR | 4 digit year |
| I94MON | numeric month |
| I94CIT | 3 digit code for country of origin |
| I94RES | 3 digit code for country of residence |
| I94PORT | port of entry |
| ARRDATE | arrival date in the USA |
| I94MODE | mode of transportation: 1 = air, 2 = sea, 3 = land, 4 = not reported) |
| I94ADDR | state of arrival |
| DEPDATE | departure date from the USA |
| I94BIR | age of respondent in years |
| I94VISA | visa codes collapsed into three categories: 1 = Business; 2 = Pleasure; 3 = Student |
| COUNT | summary statistics |
| DTADFILE | date added to I-94 files |
| VISAPOST | department of state where where Visa was issued |
| OCCUP | occupation that will be performed in U.S. |
| ENTDEPA | arrival flag: admitted or paroled into the US |
| ENTDEPD | departure flag: departed, lost I-94, or deceased |
| ENTDEPU | update flag: apprehended, overstayed, or adjusted to perm residence |
| MATFLAG | match flag: match of arrival and departure records |
| BIRYEAR | 4 digit year of birth |
| DTADDTO | character date field: date to which admitted to U.S. (allowed to stay until)  |
| GENDER | non-immigrant sex |
| INSNUM | INS number |
| AIRLINE | airline used to arrive in U.S. |
| ADMNUM | admission number |
| FLTNO | flight number of airline used to arrive in U.S. |
| VISATYPE | class of admission legally admitting the non-immigrant to temporarily stay in U.S. |

### Demographics Data

| Column Name | Description |
| :--- | :--- |
| City | the city |
| State | the state |
| Race | the race of the given entry |
| Count | number of people that identify of this race |
| Median Age | the median age |
| Male Population | the male population count |
| Female Population | the female population count |
| Total Population | the total population count |
| Number of Veterans | the number of veterans |
| Foreign-born | the number of foreign-born people |
| Average Household Size | the average household size |
| State Code | the two letter state code |

### 5.2. World Temperature Data

| Column Name | Description |
| :--- | :--- |
| dt | ID |
| AverageTemperature | average land temperature in celsius |
| AverageTemperatureUncertainty | the 95% confidence interval around the average |
| State | state |
| Country | country |

## Data Model

### Purpose of data model

We want to provide a star schema that allows analytics operations to answer questions such as:

- Gender distribution of immigrants per state
- Average temperature of country of origin versus state of arrival
- Median age of immigrants versus median age of state of arrival
- What is the most prevalant race in state of arrival versus the country of origin

### Schema

![alt text](immigration-data-lake.png "Immigration Data Lake Schema")

Here is the final schema of our data model.
### Immigration Facts Table

This dataset also comes with the file I94_SAS_Labels_Descriptions.SAS that describes the labels. We extract from this file a table that maps the 3 digit country codes to the actual country names. We clean it up and create a table of mappings in csv format. It contans the following fields:

| code | 3 digit code of country
| country | country name

We also mapped the transportation type using the following mapping:

```
{1: 'air', 2: 'sea', 3: 'land', 4: 'not reported'}
```

The date fields like arrival date and departure date are SAS numeric fields so they were converted to datetime.

### Person Dimension Table

For the person dimension table we extracted the fields shown in the schema above. Using the description file, we created a country code mapping table to map the country codes to the actual country names. The person data can be linked to the immigration data using the id.

### Demographics Dimension Table

We extracted the data from the demographics data source. The demographics data can be linked to the immigration data using the state.

### Weather Dimension Table

We extracted the data from the demographics data source. The weather data can be linked to the immigration data using the state.

## Data Quality Checks

In the pyspark-etl script, we throw errors if any of the fact or dimension tables has a length of 0.

## Project Setup

These are the required steps to setup the project. Make sure to issue all commands from the root directory of the project.

### Setup environment

Install pip/conda environment from data-lake-requirements.txt file
### AWS CLI

Follow the instructions [here](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html#cliv2-linux-install).

Or simply as mentioned in the above link, use the following commands.

```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

### AWS Configure

#### Generate Credentials

If you don't have your access key, follow the guide [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-config).

Or follow these steps:

- Go to my security crendetials on aws dashboard
- Then Access Keys
- Create new access key

#### Configure AWS for AWS CLI

Type the following command:

```
aws configure
```

Enter your access key id and secret access key. Choose your region and use json for output format.

### Create EMR Roles

Create EMR_DefaultRole using this command:

```
aws emr create-default-roles
```

Check if the roles were properly created using

aws iam list-roles

### 6.1. Setup Docker and Docker-Compose

Setup [Docker](https://docs.docker.com/engine/install/) and [Docker-Compose](https://docs.docker.com/compose/install/)

### 6.3. Pyspark

Run jupyter/pyspark-notebook docker

Make sure you are located in the project **root** directory before running these commands.

Use the following command:

```
docker run -it -p 8888:8888 jupyter/pyspark-notebook
```

Use the -v flag to persist the data generated. Map a host folder, in our case the root project folder to the docker work folder. The --rm flag removes the image after we exit, it is not mandatory.

```
docker run -it --rm -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/pyspark-notebook
```

### Setup AWS

Make sure to download all the data from the links provided above. The country code mapping table should be copied from the I94 field description file to a csv.

Run this python script to setup S3 bucket and copy all the data to it.

```
python infrastructure_setup.py
```

### Airflow

To run airflow docker use:

```
docker-compose -f docker-compose-LocalExecutor.yml up -d
```

To shutdown airflow docker use:

```
docker-compose -f docker-compose-LocalExecutor.yml down
```

### Reminder

Shut down all EMR clusters and S3 buckets manually in case of error.

## References

[1] https://hub.docker.com/r/jupyter/pyspark-notebook
[2] https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/
[3] https://github.com/puckel/docker-airflow
