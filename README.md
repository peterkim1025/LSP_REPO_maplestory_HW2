# LSP_REPO_maplestory_HW2
# ETL Pipeline Assignment

## Overview

This project implements a simple ETL (Extract, Transform, Load) pipeline in Java. The pipeline reads product data from a CSV file, applies business logic transformations, and writes the results to a new CSV file. It also prints a summary of the ETL process.

## How It Works

- **Extract:** Reads product data from `data/products.csv`.
- **Transform:** 
  - Converts product names to uppercase.
  - Applies a 10% discount to electronics.
  - Renames the category to "Premium Electronics" if the discounted price is above $500.
  - Adds a "PriceRange" column based on price.
- **Load:** Writes the transformed data to `data/transformed_products.csv`.

## Usage

1. Place your input CSV file at `data/products.csv` with the following header:
   ```
   ProductID,Name,Price,Category
   ```
2. Compile and run the Java program:
   ```sh
   javac src/org/howard/edu/lsp/assignment2/ETLPipeline.java
   java -cp bin ETLPipeline