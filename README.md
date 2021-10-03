# Lunar Assignment

This document contains all the information and explanation of the code and process followed in order to complete the assignment given by Lunar for the Data Engineering role. Therefore, I would like to thank beforehand to Lunar for the chance to perform such a assignment.
 
 
 ## Fetch the data
The first task of the assignment consistis in fetching the data files contained in the Google Clound Platform (GCP) bucket named **'de-assignment-data-bucket'**. Like recommended by the instructions, the gsutil was used in order to fetch the data. However, before using gsutil is necessary to install it (information in [gsutil install](https://cloud.google.com/storage/docs/gsutil_install#install)). After installing **gsutil** and set correctly the file directory to the GitHub directory, we can finally start with downloading the data.
 
Using the following code line on the comandline is possible to download the entire set of data in the designated bucket. However, it performs it in a single batch.

```
gsutil cp -r gs://de-assignment-data-bucket/data .
```

However, according to gsutil documentation [gsutil commands](https://cloud.google.com/storage/docs/gsutil/commands/cp):

> If you have a large number of files to transfer, you can perform a parallel multi-threaded/multi-processing copy using the top-level gsutil -m option (see gsutil help options):


Therefore the following line was used to download the entire data in batches of 25, accelarating the process of fetching as it performs a parallel multiprocessing copy. The line can be run directly on the terminal after the working directory is correctly set.

```
gsutil -m -o "Boto:parallel_thread_count=25" cp -r gs://de-assignment-data-bucket/data .
```

As an end result of this process, we obtain a folder called 'data' with 3 999 '.csv' files.

## Data transformation

This reprensents the second part of the assignment which consists in transforming the '.csv' files according to the instructions given. The code for this part of the transformation can be found in 'AssignmentCode.py'. As desired, the code generates files per craf per planet, meaning 4 output files which are saved in the main directory of the repository. The code contains 3 supplementary functions (*getdate*, *label_size*, *transform*) which are then integrated into to the main.

1. **getdate** - A function that receives the file name and returns the numbers bellonging to the date in the correct order to be converted into *yyyy-MM-dd HH:mm:ss* format;

2. **label_size** - A function that synthesizes the conditions necessary to apply to column 'id' in order to obtain the 'magnitude' labeling. It is applied with a lambda function so it receives each row at a time and applies its conditions;

3. **transform** - A function that receives the '.csv' already in a Dataframe format and applies the many transformations asked in the instruction. It uses the previous function function *label_size*;


## Testing 

As part of the testing phase, there are two testing types that were performed: unit test and integration test. Regarding unit test, is more focused on testing small parts of the code individually in order to understand if they are performing the correctly. As for the integration test it evaluates the overall performance of the code. Both parts of test are evaluated and present in another folder inside the repository called 'testdata'.

Regarding the unit test, only the function **label_size** was tested. In order to do so, the package *unittest* was used. The package allows to input a specific value and a corresponding expected result.

In the integration test, three .csv files were written:'rocket_venus_20001010_101012.csv', 'lander_saturn_20001010_101011.csv' and 'lander_saturn_20001010_101010.csv'. Moreover, another .xlsx was added to the folder in order to see if the code runs other files by mistake. 

The files contain column names in with uppercase. 

The result is expected to be two .csv files, 'rocket_venus.csv' and 'lander_saturn.csv'. 
| id   | size | core               | speed             | force              | clones | timestamp           | magnitude |
|------|------|--------------------|-------------------|--------------------|--------|---------------------|-----------|
| 100e | 1    | 86.68..  | 27.25...| 76.79..  | 10     | 2000-10-10 10:10:10 | tiny      |
| 003o | 100  | 54.38.. | 82.06... | 52.91..  | 5      | 2000-10-10 10:10:11 | big       |
| 002e | 50   | 82.93...  | 2.68... | 11.39.. | 6      | 2000-10-10 10:10:11 | medium    |
| 001e | 1000 | 74.83..   | 72.85.. | 64.43..   | 7      | 2000-10-10 10:10:10 | massive   |


| id   | size  | speed              | axis_angle        | timestamp           | magnitude |   |
|------|-------|--------------------|-------------------|---------------------|-----------|---|
| 006p | -1.0  | 46.25..  | 77.58.. | 2000-10-10 10:10:12 | small     |   |
| ll06 | 561.0 | 12.40.. | 33.39.. | 2000-10-10 10:10:12 | massive   |   |



