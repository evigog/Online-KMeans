## Online-K-Means
My Master Thesis project with title "A review of distributed stream processing systems and implementation of Streaming K-Means on Apache Flink"

# Execution Requirements
  scala version : 2.10.4
  flink version : 1.1.4
  project build wth maven 3.3.9

# Execution Instructions (standalone mode)
  1. Download Apache Flink [version 1.1.4](http://flink.apache.org/downloads.html)
  2. Put data file (week.csv) inside folder flink-1.1.4
  3. Build project : mvn clean install
  4. Execute (inside folder flink-1.1.4) :
  bin/flink run -c evi_thesis.flink.clustering.OnlineKMeansJob <path to file flink-online-kmeans-1.1.4- fat.jar>
  5. Folder flink-1.1.4/Results contains the output files:
    - TrainDataStream.txt : input data
    - TrainedModelStream.txt : output models
    - Partition.txt : each row contains each input element and info about the cluster in which it was
    assigned
    - NewCentroids.txt : new centroids computed
    - OverallModel.txt : models from each round
    - EvaluateClustering.txt : each row contains the value of distance from each centroid to
    hypercentroid
    - MaxMin.txt : contains the ratios computed for all models
# Visualization
  1. Install Elasticsearch and Kibana, following the instructions [here](http://training.data-artisans.com/elastic.html)
  2. Create elasticsearch index, executing the commands found in elasticsearch_index.txt (project
  folder)
  3. Execute (inside folder flink-1.1.4) :
  bin/flink run -c evi_thesis.flink.clustering.visualization.ClusterVisualization <path to file flink-online- kmeans-1.1.4-fat.jar>
# Dataset
  Full dataset found here (train.csv.zip)
  File week.csv found in this folder contains only data from 1.7.13 - 7.7.2013 Input file must contain elements with ascending alignment by field TIMESTAMP
