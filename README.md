# Betfair Historical Data

A Jupyter notebook to explore, analyse and visualise Betfair historic data using PySpark.

#### Installation
The following packages should be installed.
~~~
pip install pyspark==2.3.3
pip install bz2file
pip install glob2
pip install jupyter
~~~
See `requirements.txt` for list of installed packages.

#### Obtaining Data
Historical data may be downloaded from the Betfair website as follows.
1. Request and downloaded the data you want to analyse from this area of Betfair website,
https://historicdata.betfair.com/#/mydata
2. Create a folder `data` in the project folder.
3. The `data.tar` file should be opened using your choice of file compression tool - for example Z-Zip. Using the tool, navigate to `data.tar\C:\data\xds\historic\BASIC\`, and then extract to the folders to the project's `data` folder.

#### Useful Resources
https://historicdata.betfair.com/Betfair-Historical-Data-Feed-Specification.pdf
