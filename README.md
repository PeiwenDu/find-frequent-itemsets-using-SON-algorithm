# Find frequent itemsets using SON algorithm

## Project description
The project is a mini-project in class data mining. The goal is to implement the SON algorithm to find frequent itemsets in two datasets.

## Program language and libraries
Python, Spark, only use standard Python libraries and Spark RDD

## Procedure
- Create a basket for each user containing the business ids reviewed by this user
- Filter out qualified users who reviewed more than k businesses
- Apply the SON algorithm code to the filtered market-basket model

## Data
Acquired from Yelp dataset
