cascading-maxmind
=================
Geo lookup function for cascading, uses MaxMind GeoIPCity.


## Usage ##
=================
	import buryat.cascading.geo.GeoLookup;
	
	…
	
	Pipe countriesAndCities = new Each(
		inputPipe,
		new Fields("ip"),
		new GeoLookup(new Fields("country", "city"))
	);
	
Remember that GeoLookup uses GeoIPCity.dat file from Hadoop DistributedCache, so you need to add the file to the cache before using the function.

##Example project##
=================
I've included an example project, which shows how Hadoop DistributedCache and GeoLookup can be used, there's also a small test for the function.

	mvn clean package
	
	hadoop jar ./target/cascading-maxmind-0.1.jar hdfs:///GeoIPCity.dat input output
	

![Project's flow](http://sedictor.ru/13/06/12/1371023185.png "Project's flow")
	
	
##Tested on##
=================
* Cascading 2.1.6
* Hadoop 2.0.0-mr1-cdh4.2.1
* Maven 3.0.4

##License##
=================
Licensed under the MIT license