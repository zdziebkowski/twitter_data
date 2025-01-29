# Twitter Data Preparation with Apache Spark
This project focuses on preparing Twitter data using Apache Spark to facilitate efficient analysis. It encompasses data cleaning, transformation, and loading processes to ensure the data is ready for insightful analytics.

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)

## Project Overview
The primary goal of this project is to streamline Twitter data processing using Apache Spark. By leveraging Spark's distributed computing capabilities, the project efficiently handles large datasets, performing necessary cleaning and transformation steps to prepare the data for analysis.

## Features
Data Cleaning: Remove duplicates, handle missing values, and filter irrelevant information.
Data Transformation: Normalize text, extract relevant features, and structure data appropriately.
Data Loading: Save the processed data in a format suitable for analysis.

## Installation
To set up the project locally, follow these steps:
1. Clone the Repository:
```
git clone https://github.com/zdziebkowski/twitter_data.git
cd twitter_data
```
2. Set Up a Virtual Environment (optional but recommended):
```
python3 -m venv venv
source venv/bin/activate  # On Windows, use venv\Scripts\activate
```
3. Install Dependencies:
```
pip install -r requirements.txt
```
Note: Ensure that Apache Spark is installed and properly configured on your system.

## Usage
To execute the data preparation pipeline:
1. Configure Twitter API Credentials:
Ensure that your Twitter API credentials are set up correctly in the project. This typically involves setting environment variables or configuring a credentials file.
2. Run the Main Application:
```
python twitter_app.py
```
This script will initiate the data extraction, cleaning, transformation, and loading processes.

## Project Structure
The project is organized as follows:
```
twitter_data/
├── analysers/          # Modules for data analysis
├── cleaners/           # Modules for data cleaning
├── loaders/            # Modules for data loading
├── twitter_app.py      # Main application script
├── pyproject.toml      # Project configuration
└── requirements.txt    # Python dependencies
```
## Contributing
Contributions are welcome! If you'd like to contribute, please fork the repository, create a new branch for your feature or bug fix, and submit a pull request.
