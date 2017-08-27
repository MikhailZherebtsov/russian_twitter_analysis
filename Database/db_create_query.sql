DROP DATABASE twitterdb;
CREATE DATABASE IF NOT EXISTS twitterdb;

CREATE TABLE IF NOT EXISTS twitterdb.event_summary(
	id MEDIUMINT NOT NULL AUTO_INCREMENT,
    table_name_in_db TEXT,
    date_start DATETIME DEFAULT NULL,
    date_end DATETIME DEFAULT NULL,
    sample_type TEXT,
    sample_modularity DECIMAL(8,7) DEFAULT NULL,
    done VARCHAR(1) DEFAULT NULL,
    num_tweets BIGINT(20) DEFAULT NULL,
    num_rts BIGINT(20) DEFAULT NULL,
    num_users BIGINT(20) DEFAULT NULL,
    PRIMARY KEY (id)
    );
CREATE TABLE IF NOT EXISTS twitterdb.usersMaster (
	userid VARCHAR(18) PRIMARY KEY,
    username TEXT,
    givenname TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
    acc_createdat DATETIME,
    user_lang VARCHAR(7),
    done INTEGER
    );
CREATE TABLE IF NOT EXISTS twitterdb.linksMaster (
	userid1 VARCHAR(18),
    userid2 VARCHAR(18),
    time_stamp TIMESTAMP,
	PRIMARY KEY(userid1, userid2)
	);
CREATE TABLE IF NOT EXISTS twitterdb.twtMaster (
	twtid VARCHAR(18) PRIMARY KEY,
    userid VARCHAR(18),
    twttext TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
    twt_createdat DATETIME,
    twt_lang VARCHAR(7)
    );
CREATE TABLE IF NOT EXISTS twitterdb.rtMaster (
	twtid VARCHAR(18) PRIMARY KEY,
    userid VARCHAR(18),
    rt_createdat DATETIME,
    rttwtid VARCHAR(18)
    );
CREATE TABLE IF NOT EXISTS twitterdb.qtMaster (
	twtid VARCHAR(18) PRIMARY KEY,
    qttwtid VARCHAR(18)
    );
CREATE TABLE IF NOT EXISTS twitterdb.hashMaster1 (
	twtid VARCHAR(18),
    hashid VARCHAR(10)
    );
CREATE TABLE IF NOT EXISTS twitterdb.hashMaster2 (
	hashid VARCHAR(10) PRIMARY KEY,
    hashtag TEXT
    );
CREATE TABLE IF NOT EXISTS twitterdb.urlMaster1 (
	twtid VARCHAR(18),
    urlid VARCHAR(10)
    );
CREATE TABLE IF NOT EXISTS twitterdb.urlMaster2 (
	urlid VARCHAR(10) PRIMARY KEY,
    url TEXT
    );
CREATE TABLE IF NOT EXISTS twitterdb.mentionsMaster (
	twtid VARCHAR(18),
    userid VARCHAR(18)
    );
CREATE TABLE IF NOT EXISTS twitterdb.repliesMaster (
	twtid VARCHAR(18),
    inreplytotwtid VARCHAR(18)
    );
CREATE TABLE IF NOT EXISTS twitterdb.bio (
	userid VARCHAR(18) PRIMARY KEY,
    bio TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
    );
CREATE TABLE IF NOT EXISTS twitterdb.twt_numbers (
	twtid VARCHAR(18) PRIMARY KEY,
    favcount INTEGER,
    rtcount INTEGER,
    time_stamp TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP
    );
CREATE TABLE IF NOT EXISTS twitterdb.user_numbers (
	userid VARCHAR(18) PRIMARY KEY,
    numfriends INT,
    numfollowers BIGINT,
    time_stamp TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP
    );
CREATE TABLE IF NOT EXISTS twitterdb.streaming_ids (
	twtid VARCHAR(18) PRIMARY KEY
    );
CREATE TABLE IF NOT EXISTS twitterdb.coordinates (
	twtid VARCHAR(18) PRIMARY KEY,
    geolat FLOAT(10,6),
    geolon FLOAT(10,6)
    );