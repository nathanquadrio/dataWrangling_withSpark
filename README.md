# Sparkify Log Data Analysis

This script processes a user activity log from the Sparkify app using PySpark. It performs various data wrangling and analysis tasks such as counting rows, filtering data, performing aggregations, and calculating statistics. The script is designed to analyze user behavior and interactions with the app.

## Prerequisites

- Python 3.x
- PySpark
- Java Development Kit (JDK) 11
- Additional Python libraries: `numpy`, `pandas`, `matplotlib`

## Setup

1. **Install Python Libraries**:
   Ensure the required Python libraries are installed. You can install them using `pip`:
   ```bash
   pip install pyspark numpy pandas matplotlib
   ```

2. **Set JAVA_HOME**:
   Set the `JAVA_HOME` environment variable to the path of JDK 11. For example:
   ```bash
   export JAVA_HOME="/path/to/jdk-11"
   ```

3. **Data File**:
   Place your `sparkify_log_small.json` file in the `./data/` directory or update the `input_path` variable in the script to point to the correct location.

## Script Explanation

The script performs the following steps:

### 1. Imports and Configurations
- Import necessary libraries and modules.
- Configure logging to capture the script's progress and errors.
- Set the `JAVA_HOME` environment variable.

### 2. Initialize Spark Session
- Create a Spark session named "Wrangling Data".

### 3. Read Data
- Read the JSON log file into a PySpark DataFrame.

### 4. Data Verification and Exploration
- Various commented-out lines show different ways to explore and verify the data, such as printing schema, showing sample data, and describing statistics.

### 5. Feature Engineering
- Create a user-defined function (UDF) to extract the hour from the timestamp.
- Add an "hour" column to the DataFrame using this UDF.

### 6. Filtering and Grouping Data
- Filter the data to include only "NextSong" page actions.
- Group by hour and count the number of songs played in each hour.

### 7. Data Cleaning
- Drop rows with missing values in `userId` and `sessionId`.
- Filter out rows where `userId` is an empty string.

### 8. User Downgrade Analysis
- Create a UDF to flag downgrade events.
- Add a "downgraded" column using this UDF.
- Use window functions to identify phases before and after downgrades.

### 9. Page Visits Analysis
- Determine pages not visited by users with an empty `userId`.
- Log the results.

### 10. Gender Analysis
- Count the number of distinct female users and log the result.

### 11. Artist Play Count Analysis
- Filter out rows with missing artist information.
- Group by artist and count the number of plays.
- Identify and log the most frequently played artist and their play count.

### 12. Average Song Plays Between Home Visits
- Filter the dataset to include only "Home" and "NextSong" pages.
- Order the dataset by timestamp and user ID.
- Use a cumulative sum to count "Home" visits.
- Count the number of "NextSong" actions between "Home" visits.
- Compute and log the average number of "NextSong" actions between "Home" visits.

### 13. Exception Handling
- Capture and log any exceptions that occur during execution.

### 14. Stop Spark Session
- Stop the Spark session to free up resources.

## Running the Script

1. Ensure you have set up the prerequisites.
2. Run the script:
   ```bash
   python data_wrangling.py
   ```
