CREATE TABLE IF NOT EXISTS prodotti
(id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT,
helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\\';

LOAD DATA LOCAL INPATH './esercizi/Reviews.csv' OVERWRITE INTO TABLE prodotti;

DROP TABLE IF EXISTS task3;
CREATE TABLE task3 AS



SELECT
t1.productId AS item1,
t2.productId AS item2,
COUNT(1) AS cnt
FROM
(
SELECT DISTINCT userId, productId
FROM prodotti
) t1
JOIN
(
SELECT DISTINCT userId, productId
FROM prodotti
) t2
ON (t1.userId = t2.userId)
GROUP BY t1.productId, t2.productId
HAVING t1.productId > t2.productId
ORDER BY t1.productId DESC
