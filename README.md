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
    <li>Make sure you have MySQL server installed</li>
    <li>Run the <i>db_create_query.sql</i> - create the schema to be used for normalized data</li>
  </ol>
  <li><b>Data collection:</b></li>
  <p>Data collection can be done in 2 main ways: streaming and/or search (which can also be filtered to limit by lanugage only or by keywords. <b>NOTE</b>: All search methods require data normalization and distribution to the relevant MySQL DB tables. This is done by the <i>MySQL_CategorizeNSave.py</i> script.</p>
  <ul>
    <li><i>SEARCH.py</i> - collect data from the REST API by searching for specific keywords;</li>
    <li><i>STREAMING-ALL.py</i> - stream data from the Streaming API filtered only by a specific language (to get an overall impact of all discussions in a speicific language).</li>
    <li><i>Friendship_collection.py</i> - once above data is collected, the follow/friend relationships of all users in the captured sample(s) can be obtained for SNA purposes.</li>
  </ul>
  <li><b>Analysis:</b></li>
  <ol>
    <li><i>Detect_communities.py</i> - automatic detection of communities based on friendships collected using the infomap method;</li>
    <li><i>Conversation_polarization.py</i> - create cross- and intra- community statistics to evaluate homophily
  </ol>
</ol>
