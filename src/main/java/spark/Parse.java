package spark;

import utils.AmazonFFRConstants;

public class Parse {

    private static String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
    private static Review review = new Review();
    private static String summary;

    public static Review parseCsvToReview(String csvLine) {
        String[] parts = csvLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        summary = parts[AmazonFFRConstants.SUMMARY];
        String cleanLine = summary.toLowerCase().replaceAll(tokens, " ");
        review.setSummary(cleanLine);
        return review;
    }

}
