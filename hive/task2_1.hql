CREATE TABLE IF NOT EXISTS prodotti (id INT, productId STRING, userId STRING, profileName STRING, helpfulnessNumerator INT, helpfulnessDenominator INT, score INT, time BIGINT, summary STRING, text STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

 LOAD DATA LOCAL INPATH './Reviews.csv' OVERWRITE INTO TABLE prodotti;


DROP TABLE IF EXISTS task1;


CREATE TABLE task1 AS
SELECT result.year,
       result.word,
       result.freq
FROM
  (SELECT res.year AS YEAR,
          res.word AS word,
          res.COUNT AS freq,
          ROW_NUMBER() OVER(PARTITION BY res.YEAR
                            ORDER BY res.COUNT DESC) AS row_nr
   FROM
     (SELECT w.YEAR,
               w.word,
               count(1) AS COUNT
      FROM
        (SELECT r.YEAR AS YEAR,
                          word.splitted AS word
         FROM
           (SELECT from_unixtime(time,'yyyy') AS YEAR,
                   LTRIM(RTRIM(LOWER(REGEXP_REPLACE(summary, '[^0-9A-Za-z]', ' ')))) AS word
            FROM prodotti) r LATERAL VIEW explode(split(r.word, ' ')) word AS splitted) w
      WHERE w.YEAR >1998
        AND w.YEAR <2013
        AND w.word!=""
      GROUP BY w.word,
               w.YEAR
      ORDER BY w.YEAR,
                 COUNT DESC) res) result
WHERE result.row_nr >0
  AND result.row_nr <11;

