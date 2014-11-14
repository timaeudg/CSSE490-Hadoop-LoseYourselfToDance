DROP DATABASE IF EXISTS MillionSongData CASCADE;
CREATE DATABASE IF NOT EXISTS MillionSongData;

USE MillionSongData;

DROP DATABASE IF EXISTS BottomTenPercentAggregates;
CREATE TABLE IF NOT EXISTS BottomTenPercentAggregates(
field varchar(30) NOT NULL,
mean_value double NOT NULL
);

DROP DATABASE IF EXISTS TopTenPercentAggregates;
CREATE TABLE IF NOT EXISTS TopTenPercentAggregates(
field varchar(30) NOT NULL,
mean_value double NOT NULL
);

DROP DATABASE IF EXISTS FullAggregates;
CREATE TABLE IF NOT EXISTS FullAggregates(
field varchar(30) NOT NULL,
mean_value double NOT NULL
);