<h1>Covid Simulation

<h2>Files Description </h2> <br>
<b>Requirements.txt</b> has all pip dependencies used in the project <br>
<b>Makefile</b> creates a dist.zip files with needed files to run spark scripts <br>
<b>src/bash/pre-install.sh</b> bash script to install pip dependencies on data-proc cluster <br>

<h2>Considerations </h2> <br>
<b>main.py</b> has hardcoded paths to input files, both root path and partition paths, and also has hardcoded 
ouput files names <br>

<h2>Run instructions</h2>
1. First creates zip dependencies using ```make``` on root directory
2. Run pyspark scripts. eg. on data-proc 
```gcloud dataproc jobs submit pyspark --cluster=cluster-spark-test --region=us-central1 --py-files dist.zip src/main.py```
3. Results stats files are under `results/`. There are two files: <br>
    a. `daily_infected.csv` <br>
    b. `daily_geoloc_evts.csv`
4. To display results graphically use the results files.<br>
    a. To display daily_geoloc_evts  `python src/graph/histogram_3d.py -inp daily_geoloc_evts.csv` <br>
    b. To display daily_infected `python src/graph/line_chart.py -inp daily_infected.csv`