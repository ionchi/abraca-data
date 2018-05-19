Job 1 spark

        run(inputPath,ouputPath)
        Input: inputPath: directory containing input files,
        outputPath: directory where the output will be written;
        create the Spark context and load input from inputPath
        inputRDD <-- each row loaded extract year and summary
                create tuple <year , summary>
                group by year
        for each tuple extract word
                create new tuples as <word,1>
                group by word and sum
                sort by occurence of each word
                group by year
                get the first 10 words for yeach year
                save as a text file in outputPath


Job 2 spark

        run(inputPath,ouputPath)
        Input: inputPath: directory containing input files,
        outputPath: directory where the output will be written

        create the Spark context and load input from inputPath
        inputRDD <--
                {each row loaded extract year,productId score
                create tuple(<year +"\t"productId>,<score,1>)
                }
        inputRDD<--for each tuple compute the average score of the key
        inputRDD<--new tuple (productID, year+ average score)
                group by product
                select years between 2003 and 2012 (include ends)
                sort by product
                save as a text file in outputPath




Job 3 spark

        run(inputPath,ouputPath)
        Input: inputPath: directory containing input files,
        outputPath: directory where the output will be written;

        create spark context
        create sql context and link with spark context
        load input from inputPath
        people<-- productId and userId from inputPath
        save people into Amazontable
        link sql context with sparkDataframe
        sparkDataFrame <-- (SELECT t1.productId AS item1,t2.productId AS item COUNT(1) AS cnt
                            FROM t1{
                                SELECT DISTINCT userId, productId
                                FROM people}
                            JOIN t2{
                                SELECT DISTINCT userId, productId
                                FROM people}
                            ON (t1.userId = t2.userId)
                            GROUP BY t1.productId, t2.productId
                            HAVING t1.productId > t2.productId
                            ORDER BY t1.productId ASC
        convert sparkDataFrame in RDD and save as text in outputPath



