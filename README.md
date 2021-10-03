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

2. **label_size** - A function that synthesizes the conditions necessary to apply to column 'id' in order to obtain the 'magnitude' labeling. It is applied with a lambda function so it receives each row at a time and applies its conditions; In this point it was noted that some of the records on the csv files had values bellow 1 or equal to 1000. According to the conditions imposed by the exercise these values do not enter in any category, and therefore a different message appears.

3. **transform** - A function that receives the '.csv' already in a Dataframe format and applies the many transformations asked in the instruction. It uses the previous function function *label_size*;


## Testing 

As part of the testing phase, there are two testing types that were performed: unit test and integration test. Regarding unit test, is more focused on testing small parts of the code individually in order to understand if they are performing the correctly. As for the integration test it evaluates the overall performance of the code. Both parts of test are evaluated and present in another folder inside the repository called 'testdata'.

Regarding the unit test, only the function **label_size** was tested. In order to do so, the package *unittest* was used. The package allows to input a specific value and a corresponding expected result, as it can be observed in the *TestCode*


In the integration test, three .csv files were written:'rocket_venus_20001010_101012.csv', 'lander_saturn_20001010_101011.csv' and 'lander_saturn_20001010_101010.csv'. Moreover, another .xlsx was added to the folder in order to see if the code runs other files by mistake. 

The files contain the following information respectively:


rocket_venus_20001010_101012.csv:

| id                                   | SIZe | speed              | axis_ANGLE        |
|--------------------------------------|------|--------------------|-------------------|
| 6173a932-3c6a-005p-ad70-0c54ce356bc6 | kpud | 17.25943911932356  | 38.86402374489981 |
| 71ea9ca7-f5a1-006p-badd-f626d94f3126 | 9    | 46.25116735310198  | 77.58275002757036 |
| 1f6d9491-69a8-ll06-b0a9-a06adc96c70a | 49   | 12.400358957016325 | 33.39925972666718 |

lander_saturn_20001010_101011.csv:
| id                                   | size  |  core              |  SPEED            | force              | clones|
|---------------------------------------|------|--------------------|-------------------|--------------------|---|
| 02bfeb64-57b8-002e-9049-89cf38b93320  | 50   | 82.93940010212984  | 2.681232883995321 | 11.397628863973033 | 6 | 
| 50dfd2e9-fdsdf-003o-ae03-e6f2fcf1f71c | 100  | 54.380011078858224 | 82.06253713668954 | 52.91517980722712  | 5 |

lander_saturn_20001010_101010.csv:
| id                                   | size | CoRe              | SPEED              | force             | CLoNeS |
|--------------------------------------|------|-------------------|--------------------|-------------------|--------|
| 78e1b0cf-fd89-100e-a648-55caa09778d5 | 1    | 86.68229942222254 | 27.253772429555053 | 76.79263249355178 | 10     |
| 7a42ee5c-0af4-001e-8dce-d91af0937c4d | 1000 | 74.8383608624044  | 72.85814727325068  | 64.4383895722071  | 7      |







The result is expected to be two .csv files, 'rocket_venus.csv' and 'lander_saturn.csv' present in the *testdata* file.
| id   | size | core               | speed             | force              | clones | timestamp           | magnitude |
|------|------|--------------------|-------------------|--------------------|--------|---------------------|-----------|
| 002e | 50   | 82.93940010212984  | 2.681232883995321 | 11.397628863973033 | 6      | 2000-10-10 10:10:11 | medium    |
| 003o | 100  | 54.380011078858224 | 82.06253713668954 | 52.91517980722712  | 5      | 2000-10-10 10:10:11 | big       |
| 100e | 1    | 86.68229942222254  | 27.25377242955505 | 76.79263249355178  | 10     | 2000-10-10 10:10:10 | tiny      |
| 001e | 1000 | 74.8383608624044   | 72.85814727325068 | 64.4383895722071   | 7      | 2000-10-10 10:10:10 | massive   |


| id   | size  | speed              | axis_angle        | timestamp           | magnitude |
|------|-------|--------------------|-------------------|---------------------|-----------|
| 006p | 9  | 46.25..  | 77.58.. | 2000-10-10 10:10:12 | small     |
| ll06 | 49 | 12.40.. | 33.39.. | 2000-10-10 10:10:12 | medium   |


## Conclusion

During this assignment I was able to explore and learn about some methodologies and processes which were new for me, regarding the tasks of a data engineer. The structured way of working implied in the task's organization (Fetch Data - Transform - Test) determined a very structured way of working, which helped me a lot. The tasks required a lot of reading and searching, particularly for the first and last part of the assignmemnt. A lot of this commands and processes were new to me so it required more time. Besides that, using GitHub was something relatively new, as I have only tried it a few times. However, I consider that challenging and it was an imense pleasure to do this assignment. I believe the results were achieved as expected and the only part where I would have spent more time would be in the testing part. Finally, thank you for the chance of having this interesting challenge.

Best regards,


Miguel Cunha

