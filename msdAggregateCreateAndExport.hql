use millionsongdataset;

DROP TABLE IF EXISTS pigOutput;
DROP TABLE IF EXISTS allSongsAggregateData;
DROP TABLE IF EXISTS bottomAggregateData;
DROP TABLE IF EXISTS topAggregateData;

CREATE TABLE pigOutput
(
year int,
song_hotttnesss double,
danceability double,
time_signature double,
tempo double,
duration double,
key double,
mode double,
energy double,
start_of_fade_out double,
end_of_fade_in double,
artist_hotttnesss double,
artist_familiarity double,
artist_name string,
title string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/root/songStatsHive' INTO TABLE pigOutput;

CREATE TABLE allSongsAggregateData
(
dataType string,
data double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE bottomAggregateData
(
dataType string,
data double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

CREATE TABLE topAggregateData
(
dataType string,
data double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Hotttnesss', AVG(song_hotttnesss) FROM pigOutput;
INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Time Signature', AVG(time_signature) FROM pigOutput;
INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Tempo', AVG(tempo) FROM pigOutput;
INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Duration', AVG(duration) FROM pigOutput;
INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Key', AVG(key) FROM pigOutput;
INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Mode', AVG(mode) FROM pigOutput;
INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Artist Hotttnesss', AVG(artist_hotttnesss) FROM pigOutput;
INSERT INTO TABLE allSongsAggregateData SELECT 'Mean Artist Familiarity', AVG(artist_familiarity) FROM pigOutput;

INSERT INTO TABLE bottomAggregateData SELECT 'Mean Hotttnesss', AVG(a.song_hotttnesss) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;
INSERT INTO TABLE bottomAggregateData SELECT 'Mean Time Signature', AVG(a.time_signature) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;
INSERT INTO TABLE bottomAggregateData SELECT 'Mean Tempo', AVG(a.tempo) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;
INSERT INTO TABLE bottomAggregateData SELECT 'Mean Duration', AVG(a.duration) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;
INSERT INTO TABLE bottomAggregateData SELECT 'Mean Key', AVG(a.key) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;
INSERT INTO TABLE bottomAggregateData SELECT 'Mean Mode', AVG(a.mode) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;
INSERT INTO TABLE bottomAggregateData SELECT 'Mean Artist Hotttnesss', AVG(a.artist_hotttnesss) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;
INSERT INTO TABLE bottomAggregateData SELECT 'Mean Artist Familiarity', AVG(a.artist_familiarity) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.10) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss <= b.butts;

INSERT INTO TABLE topAggregateData SELECT 'Mean Hotttnesss', AVG(a.song_hotttnesss) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
INSERT INTO TABLE topAggregateData SELECT 'Mean Time Signature', AVG(a.time_signature) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
INSERT INTO TABLE topAggregateData SELECT 'Mean Tempo', AVG(a.tempo) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
INSERT INTO TABLE topAggregateData SELECT 'Mean Duration', AVG(a.duration) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
INSERT INTO TABLE topAggregateData SELECT 'Mean Key', AVG(a.key) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
INSERT INTO TABLE topAggregateData SELECT 'Mean Mode', AVG(a.mode) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
INSERT INTO TABLE topAggregateData SELECT 'Mean Artist Hotttnesss', AVG(a.artist_hotttnesss) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
INSERT INTO TABLE topAggregateData SELECT 'Mean Artist Familiarity', AVG(a.artist_familiarity) FROM pigOutput a JOIN (SELECT PERCENTILE_APPROX(song_hotttnesss, 0.90) AS butts FROM pigOutput) b ON (1=1) WHERE a.song_hotttnesss >= b.butts;
