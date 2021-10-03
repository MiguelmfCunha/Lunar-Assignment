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

This reprensents the second part of the assignment which consists in transforming the '.csv' files according to the instructions given. 
