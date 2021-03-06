cascading-maxmind
=================
Geo lookup function for [cascading](http://www.cascading.org/), uses [MaxMind GeoIPCity](http://www.maxmind.com/en/city) database.


## Usage ##
```java
import buryat.cascading.geo.GeoLookup;

…

Pipe countriesAndCities = new Each(
	inputPipe,
	new Fields("ip"),
	new GeoLookup(new Fields("country", "city", "dma_code"))
);
```
	
Remember that GeoLookup uses GeoIPCity.dat file from Hadoop DistributedCache, so you need to add the file to the cache before using the function.

##Example project##
I've included an example project, which shows how Hadoop DistributedCache and GeoLookup can be used, there's also a small test for the function.

	mvn clean package
	
	hadoop jar ./target/cascading-maxmind-0.1.jar buryat.cascading.geo.Main hdfs:///GeoIPCity.dat input/ips.txt output
	

![Project's flow](https://raw.github.com/buryat/cascading-maxmind/master/dot/geo.png "Project's flow")

##Example project##
Another example project shows how cascading-maxmind might be used to process data produced by hive. I've included a sample dataset which you may run cascading-maxmind against.

	mvn clean package
	
	hadoop jar ./target/cascading-maxmind-0.1.jar buryat.cascading.geo.Date hdfs:///GeoIPCity.dat input/dates.txt output
	
![Project's flow](https://raw.github.com/buryat/cascading-maxmind/master/dot/date.png "Project's flow")
	
##Tested on##
* Cascading 2.1.6
* Hadoop 2.0.0-mr1-cdh4.2.1
* Maven 3.0.4

##License##
Licensed under the MIT license