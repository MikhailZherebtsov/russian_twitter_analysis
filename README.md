# russian_twitter_analysis
Repository of code to analyze political Twitter in Russia for political or social purposes. See http://rudatalab.com/about/ for detail about the uses of this code to analyze the politics of Russia.

<h2>Includes:</h2>
<div>
  (1) database schema creation; 
  (2) data collection from the Twitter APIs, 
  (3) analysis.
</div>

<h2>Detail:</h2> 
<div>1) Database:</div>
<div>
  A database schema is created with tables to normalize data necessary for analysis -- not all data is saved. 
</div>
<div>
  a) install MySQL server and create a database schema using db_create_query.sql
</div>
<div>2) Data collection:</div>
<div>Data collection can be done in several ways</div>
  a) Collect data from the REST API by searching for specific keywords;
  b) Stream data from the Streaming API by specific keywords; 
  c) Stream data from the Streaming API filtered only by a specific language (to get an overall impact of all discussions in a speicific language).
  d) Once above data is collected, the follow/friend relationships of all users in the captured sample(s) can be obtained for SNA purposes.
3) Analysis:
  a) Automatic detection of communities based on friendships collected using the infomap method
