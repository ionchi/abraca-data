CREATE TABLE IF NOT EXISTS prodotti
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH './esercizi/Reviews.csv'
OVERWRITE INTO TABLE prodotti;

DROP TABLE IF EXISTS task2;

CREATE TABLE task2 AS

SELECT q1.productId, q1.Year, avg(q1.score) as average
FROM
(SELECT productId,score, from_unixtime(time,'yyyy') AS Year
FROM prodotti) q1
WHERE q1.Year >2002 AND q1.Year <2013
GROUP BY q1.productId, q1.Year
ORDER BY q1.productId,q1.Year;
