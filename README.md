# Movie Analysis using Apache Spark

**Movie Analysis** is a Scala project leveraging **Apache Spark** to perform comprehensive data analytics on movie datasets. The project encompasses data preparation, various RDD-based and SQL-based analytical queries, and showcases advanced Spark features like accumulators and broadcast variables. This repository provides insights into movie trends, user ratings, genre distributions, and more.

## Table of Contents

- [Features](#features)
- [Explanation](#explanation)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [License](#license)
- [Acknowledgements](#acknowledgements)

## Features
- **Data Preperation** - Clean and transformed raw movie, user, and rating data.
- **RDD Analysis** - Performed RDD based operations to extract insights.
  
      - Top 10 Most Viewed Movies
      - Distinct List of Genres Available
      - Number of Movies for Each Genre
      - Movies Starting with Each Letter and Number
      - Latest Released Movies
- **SQL Analysis** - Utilized Spark SQL for advanced queries.
  
      - Create Hive Tables
      - Oldest Released Movies
      - Movies Per Year
      - Movies Per Rating
      - Users Per Movie
      - Total Rating Per Movie
      - Average Rating Per Movie
- **Spark Features**:
  
      - Accumulators: Track data processing metrics.
      - Broadcast Variables: Efficiently share read-only data across executors.
- **Data Import**: Download and import data from URLs.
- **Data Persistence**: Save analysis results in Parquet and CSV formats.


## Explanation 
- **DataPreparation**:
   - PrepareMovies.scala: Loads and cleans the movies data, extracting the year and splitting genres.
   - PrepareUsers.scala: Loads and cleans the users data from a double-delimited CSV.
   - PrepareRatings.scala: Loads ratings data with a programmatically specified schema.
- **RDDAnalysis**:
   - Top10MostViewedMovies.scala: Identifies the top 10 most viewed movies based on ratings count.
   - DistinctGenres.scala: Extracts and lists all distinct genres available in the dataset.
   - MoviesPerGenre.scala: Counts the number of movies available for each genre.
   - MoviesStartingWith.scala: Counts how many movies start with each letter (A-Z) and number (0-9).
   - LatestReleasedMovies.scala: Lists the latest released movies based on the release year.
- **SQLAnalysis**:
   - CreateTables.scala: Creates Hive tables from the prepared Parquet files.
   - AverageRatingPerMovie.scala: Calculates the average rating for each movie.
   - MoviesPerRating.scala: Counts the number of movies for each rating value.
   - MoviesPerYear.scala: Counts the number of movies released each year.
   - OldestReleasedMovies.scala: Lists the oldest released movies.
   - TotalRatingPerMovie.scala: Sums the total ratings received by each movie.
   - UsersPerMovie.scala: Counts the number of distinct users who rated each movie.
- **Miscellaneous**:
   - AccumulatorExample.scala: Demonstrates the use of Spark accumulators to track data processing metrics.
   - BroadcastVariableExample.scala: Shows how to use broadcast variables to efficiently share read-only data across executors.
   - ImportDataFromURL.scala: Downloads and imports data from a specified URL.
   - SaveTableWithoutDDL.scala: Saves Hive tables without explicitly defining DDL.

- **Main.scala**: The entry point of the application. It orchestrates data preparation, analytical queries, and miscellaneous tasks.
- **build.sbt**: Defines project settings, Scala version, and library dependencies.

## Prerequisites
Ensure you have the following installed on your system:

- **Java** 8 or higher
- **Scala 2.13**
- **Apache Spark 3.5.3**
- **SBT** (Scala Build Tool)
- **Git** (for cloning the repository)

## Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/MeeraLizJoy/Movie-Analysis.git
    ```
2. Navigate to the project directory:
    ```bash
    cd Movie-Analysis
    ```
3. Install the necessary dependencies:
    ```bash
    sbt install
    ```

## License
This project is licensed under the MIT License.

## Acknowledgements
- **Apache Spark**: The powerful data processing engine used for this project.
- **Scala**: The programming language used to build this project.
- **Movielens Dataset**: The dataset used for movie analysis.
- **SBT**: The build tool used for managing project dependencies and compilation.



