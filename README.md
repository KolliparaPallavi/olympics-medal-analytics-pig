# Olympics Medal Analytics with Apache Pig 🏅🐷

This project is part of my Big Data Processing coursework at RMIT University.  
It demonstrates how **Apache Pig** (a high-level platform on top of Hadoop) can be used to query and process **large-scale Olympic datasets** stored in **HDFS**.


## 📌 Project Overview
The goal of this project was to analyze Olympic medal data using **Pig scripts** and a **custom Python UDF**:
- **Task 1:** Count and rank the number of **Gold medals** per country (Region).  
- **Task 2.1:** Extend the analysis to include **Gold and Silver medal counts** per country.  
- **Task 2.2:** Use a **Python User Defined Function (UDF)** to handle missing values by replacing null counts with 0, and refine the ranking rules.



## 🛠️ Tools & Technologies
- **Hadoop HDFS** – Distributed storage for input/output data.  
- **Apache Pig** – Data processing with SQL-like scripts.  
- **Python (Jython UDF)** – Custom function to handle missing values.  
- **Olympics Dataset** – Sample CSV files with athletes, events, regions, and medal records.
