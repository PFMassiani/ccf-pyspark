Connected Component Finder in PySpark
=====================================

PySpark implementation of the MapReduce "Connected Component Finder" algorithm

# Project description

This project is the final project for the [Systems and Paradigms for Big Data class](https://www.lamsade.dauphine.fr/wp/iasd/programme/options/systemes-paradigmes-et-langages-pour-les-big-data/) of Master IASD, a PSL Research University master's programme.
It consists in a Spark implementation of the MapReduce algorithm described in [CCF: Fast and Scalable Connected Component Computation in MapReduce](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwj4puPrs_DpAhVC6RoKHT6MAGgQFjACegQIBBAB&url=https%3A%2F%2Fwww.cse.unr.edu%2F~hkardes%2Fpdfs%2Fccf.pdf&usg=AOvVaw1OSwqfiksbd0nKIdVU98bn).

# Running the code on your machine

You need to have Spark installed on your computer. An alternative is to run it on [DataBricks](https://community.cloud.databricks.com/).

## Downloading the graphs

To download some graphs the algorithm can be run upon, run in a terminal:
```
wget -P /tmp http://snap.stanford.edu/data/web-Google.txt.gz
gunzip /tmp/web-Google.txt.gz
wget -P /tmp http://snap.stanford.edu/data/cit-HepTh.txt.gz
gunzip /tmp/cit-HepTh.txt.gz
```

The graphs are now in your `/tmp` directory. You can now move them to the directory you want, but don't forget to change the paths in main.py accordingly.

### On DataBricks

Once you have downloaded the graph, execute the following in a Python notebook:
```
dbutils.fs.mv("file:/tmp/web-Google.txt", "dbfs:/FileStore/tables/web-Google.txt")  
dbutils.fs.mv("file:/tmp/cit-HepTh.txt", "dbfs:/FileStore/tables/cit-HepTh.txt")  
```
This will move the files to the distributed filesystem.

## Usage

### On DataBricks

Copy the code from `ccf_pyspark.py` and `main.py` to the notebook, and change the code from `main.py` accordingly.

### On your computer

Execute the following in a terminal:
```
spark-submit main.py --method [METHOD] --graph [GRAPH] --show [SHOW]
```
All arguments are optional. If you need explanations on the arguments, run `python main.py -h`.

# Report

The report for this project can be found in the `/doc` directory of the repository.

# References
* [Stanford Network Analysis Project (SNAP)](http://snap.stanford.edu/index.html): a C++ library for graph mining and analytics, with open access to a large number of graphs.
* [CCF: Fast and Scalable Connected Component Computation in MapReduce](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwj4puPrs_DpAhVC6RoKHT6MAGgQFjACegQIBBAB&url=https%3A%2F%2Fwww.cse.unr.edu%2F~hkardes%2Fpdfs%2Fccf.pdf&usg=AOvVaw1OSwqfiksbd0nKIdVU98bn).
* For an introduction to Secondary Sorting in MapReduce: Data-intensive text processing with MapReduce](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjXvJzxvfDpAhVkyoUKHQdBBskQFjABegQIAhAB&url=https%3A%2F%2Flintool.github.io%2FMapReduceAlgorithms%2FMapReduce-book-final.pdf&usg=AOvVaw2AOjBulu00ykxhwzSpMFZr), Chapter 3.
* For another (and more detailed) PySpark implementation of Secondary Sorting: [Spark Secondary Sort](https://www.qwertee.io/blog/spark-secondary-sort/).