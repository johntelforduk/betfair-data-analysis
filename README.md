# Betfair Historical Data

A Jupyter notebook to explore, analyse and visualise Betfair historic data using PySpark.

![Screenshot](https://github.com/johntelforduk/betfair-data-analysis/blob/master/screenshots/challenge_cup_winner_2018.jpg)

#### Installation
The following packages should be installed.
~~~
pip install pyspark==2.3.3
pip install bz2file
pip install glob2
pip install jupyter
pip install matplotlib
pip install findspark
~~~
See `requirements.txt` for list of installed packages.

#### Running The Notebook
`cd` to the folder that contains the project. Then,
~~~
(betfair) C:\betfair> jupyter notebook betfair_analysis.ipynb
~~~

#### Obtaining Data
Historical data may be downloaded from the Betfair website as follows.
1. Request and downloaded the data you want to analyse from this area of Betfair website,
https://historicdata.betfair.com/#/mydata
2. Create a folder `data` in the project folder.
3. The `data.tar` file should be opened using your choice of file compression tool - for example Z-Zip. Using the tool, navigate to `data.tar\C:\data\xds\historic\BASIC\`, and then extract to the folders to the project's `data` folder.

#### Useful Resources
https://historicdata.betfair.com/Betfair-Historical-Data-Feed-Specification.pdf
