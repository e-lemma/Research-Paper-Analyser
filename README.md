# Research Paper Collation Project

## Overview

This project is developed for a pharmaceutical company focused on improving the understanding of Sjogren Syndrome. The goal is to collate and analyse research papers from various institutions to gain a comprehensive understanding of the disease and develop effective treatments. The project involves processing PubMed XML data, cleaning and enriching it, and matching institution names with standardised names from the Global Research Identifier Database (GRID).

## Inputs

- **PubMed Dataset**: Contains search results for Sjogren Syndrome in `pubmed_result_sjogren.xml`.
- **GRID Datasets**: Provides information about public research institutions in four CSV files:
  - `institutes.csv`
  - `addresses.csv`
  - `aliases.csv`
  - `relationships.csv`

## Outputs

The final output is a CSV file containing the following columns:
- Article PMID
- Article title
- Article keywords
- Article MESH identifiers
- Article year
- Author first name
- Author last name
- Author initials
- Author full name
- Author email
- Affiliation name (from PubMed dataset)
- Affiliation name (from GRID dataset)
- Affiliation zipcode
- Affiliation country
- Affiliation GRID identifier

## Pipeline Overview

1. **Processing PubMed XML Data**: Parse the XML data and load it into memory.
2. **Data Wrangling with Pandas**: Clean and flatten the data, extracting relevant fields.
3. **Text Data and spaCy**: Use NLP techniques to enrich the dataset.
4. **Data Matching**: Match PubMed institution names with standardised GRID names.
5. **CSV Generation**: Export the processed data to a CSV file.
6. **AWS Integration**: Download input XML from S3, upload output CSV to S3, and send email notifications using SES.

## Tools

- **Python**: Main programming language.
- **Pandas**: Data manipulation and analysis.
- **spaCy**: Natural Language Processing.
- **RapidFuzz**: Text similarity metrics.
- **AWS S3**: Storage for input and output files.
- **AWS SES**: Email notifications.

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/e-lemma/Research-Paper-Analyser.git
   cd Research-Paper-Analyser
   ```

2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Environment Information

Create a `.env` file with the following environment variables:
```
ACCESS_KEY=your_aws_access_key
SECRET_ACCESS_KEY=your_aws_secret_access_key
SOURCE_BUCKET=your-input-bucket
SOURCE_KEY=path/to/your/input.xml
TARGET_BUCKET=your-output-bucket
TARGET_FOLDER=output/
```
## Running the Project

1. Ensure the necessary environment variables are set.
2. Ensure the input XML file is available in the source S3 bucket.
3. Set up the source and target S3 buckets as specified in the `.env` file.
4. Run the pipeline:
  ```sh
  python pipeline.py
  ```

## Testing

1. Create a small XML file with a few articles for testing.
2. Upload the XML file to the input S3 bucket.
3. Run the pipeline script.
4. Verify the output CSV file in the output S3 bucket.
5. Check email notifications for task status updates.

## Docker

To run the project using Docker:

1. Build the Docker image:
   ```sh
   docker build -t research-paper-analyser .
   ```

2. Run the Docker container:
   ```sh
   docker run --env-file .env research-paper-analyser
   ```