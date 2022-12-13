# Data Analysis on Vehicle Accidents in the US using Apache Spark
Analysis of US Vehicle Accidents

## Pre-requisites
1. Verifying Java Installation
2. Verifying Python installation
3. Downloading and Installing Apache Spark

## Problem Statement
Spark Application should perform below analysis and store the results for each analysis.
1. Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
2. Analysis 2: How many two wheelers are booked for crashes? 
3. Analysis 3: Which state has highest number of accidents in which females are involved? 
4. Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
5. Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
6. Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
7. Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
8. Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding roffences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the states with highest number of offences (to be deduced from the data)

## Running the project
First you will have to fork/clone this repository
The input file is currently given in the project's input/ folder, this can be changed in the config.json depending on the input file location.
```sh
cd Spark__USCrashInvestigation
spark-submit --master "local[*]" --py-files utils --files config.json main.py
```

## Sample Output
![image](https://user-images.githubusercontent.com/34810569/207415537-40884a3b-7e0a-4483-a461-b2085a834d73.png)

Along with the ouput displayed on the terminal, the output results are stored in the locations specified in the config.json as well. You can find the sample output folder structure as well as the sample output of terminal in the above screenshot.



