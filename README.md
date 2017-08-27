# russian_twitter_analysis
This is a repository of code to analyze political Twitter in Russia for political or social purposes. See http://rudatalab.com/about/ for more detail about the uses of this code to analyze the politics of Russia.

<h2>Includes:</h2>
<ol>
  <li>database schema creation;</li>
  <li>data collection from the Twitter APIs,</li>
  <li>analysis.</li>
</ol>

<h2>Detail:</h2> 
<ol>
  <li><b>Database:</b></li>
  <p>A database schema is created with tables to normalize data necessary for analysis.</p>
  <ol>
    <li>install MySQL server;</li>
    <li>Create the schema using <i>db_create_query.sql</i></li>
  </ol>
  <li><b>Data collection:</b></li>
  <p>Data collection can be done in 2 main ways: streaming and/or search (which can also be filtered to limit by lanugage only or by keywords. <b>NOTE</b>: All search methods require data normalization to the relevant MySQL DB tables. This is done by the <i>MySQL_CategorizeNSave.py</i> script.</p>
  <ul>
    <li><i>SEARCH.py</i> - collect data from the REST API by searching for specific keywords;</li>
    <li><i>STREAMING-ALL.py</i> - stream data from the Streaming API filtered only by a specific language (to get an overall impact of all discussions in a speicific language).</li>
    <li>Once above data is collected, the follow/friend relationships of all users in the captured sample(s) can be obtained for SNA purposes.</li>
  </ul>
  <li><b>Analysis:</b></li>
  <ol>
    <li>Automatic detection of communities based on friendships collected using the infomap method.</li>
  </ol>
</ol>
