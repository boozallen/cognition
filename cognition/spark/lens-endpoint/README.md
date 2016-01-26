#Build
To build an distributable tar.gz file, run:
mvn package -P dist
	
#Execute
1. Transfer and un-tar the file in its correct location: tar -zxf lens-endpoint-[version]-dist-tar.tar.gz
2. Change any configurations in the config folder
3. To execute uber jar as a http server with configurations, run from command line:
java -jar [path to lens-endpoint home]/lib/lens-endpoint-2.0.0.jar server config.yml


#URL Parameters
Parameter name | Requried | Format   | Example Parameter | Notes 
---------------|----------|----------|-------------------|------
country        | false	  |ISO 3166-1|country = US	     |
endDate	       |true	  |ISO Instant |	endDate = 2014-12-02T00:00:00.00Z	 |
keywords	|false|	String|	keywords = wordslist	 |
language	|false|	ISO 639-2/ISO 639-1 Code|	language = EN	| 
limit	|false |	int	|limit=1000 	| 
schema	|false; defaults to whatever is in lens-api.properties|	case sensitive string|	schema=gnip	 | this is the whole file name or what can be postfixed with -schema.json
startDate|	true	|ISO Instant |	startDate = 2014-12-01T00:00:00.00Z	 |
user	|false	|String	|user = user1	 |
useSpaceTokenization|	false; true by default|	boolean	|useSpaceTokenization=false|	space tokenization is used for lens queries by default for latin based languages, as they use spaces to separate words (prevents a search for US returning crush, must, etc.). However, some languages do not typically use spaces in their written form–for queries on these languages, the user must specify this property as false.

#Example REST Calls

- curl -XGET 'localhost:8080/lens?country=US&language=EN&keywords=wordslist&startDate=2015-12-01T00:00:00.00Z&endDate=2015-12-02T00:00:00.00Z'
- curl -XGET 'localhost:8080/lens?language=EN&keywords=wordslist&keywords=listed&startDate=2015-12-03T00:00:00.00Z&endDate=2015-12-04T00:00:00.00Z'

#Future Work

Initial filtering criteria (country, language, keywords, date, user) were isolated for proof of concept. Currently there is no way to enumerate extensive boolean logic (or, and, not, xor). All fields are and'd together (except keywords within the keywords list, which are or'd) at this point. Future work may allow for more extensive logic. 

SQL integration (using SparkSQL on Accumulo) is in the works and will deprecate this domain specific query language. 
