# a8-ankita

## Problem Statement

Using Spark, perform hierarchical agglomerative clustering and k-means clustering as follows:

1) Fuzzy loudness: cluster songs into quiet, medium, and loud
2) Fuzzy length: cluster songs into short, medium, and long
3) Fuzzy tempo: cluster songs into slow, medium, and fast
4) Fuzzy hotness: cluster songs into cool, mild, and hot based on song hotness
5) Combined hotness: cluster songs into cool, mild, and hot based on two dimensions: artist hotness, and song hotness

Use each method to perform each clustering task.

Compare the results, as well as the performance of the solutions.

Observe whether:

A song’s loudness, length, or tempo predict its hotness
A song’s loudness, length, tempo, or hotness predict its combined hotness


## Running Instructions

### Local

#### Prepare

1) Make sure your enviroment have installed scala, scala should have the same version as Spark build-in scala (or you can use scala path provided in repository).
2) Change `HADOOP_HOME`, `SPARK_HOME`,`SCALA_HOME`, `HADOOP_VERSION` as same as your environment.
3) To test on bigCorpus put the data in `bigInput` folder.


#### Run

- Goto `<project-root>`
- Run `make`
