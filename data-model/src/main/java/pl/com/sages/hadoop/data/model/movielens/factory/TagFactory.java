package pl.com.sages.hadoop.data.model.movielens.factory;

import pl.com.sages.hadoop.data.model.movielens.Tag;

public class TagFactory {

    private static final String DELIMITER = "::";

    public static Tag create(String line) {
        String[] data = line.split(DELIMITER);

        int userId = Integer.parseInt(data[0]);
        int movieId = Integer.parseInt(data[1]);
        String tag = data[2];
        int timestamp = Integer.parseInt(data[3]);

        return new Tag(userId, movieId, tag, timestamp);
    }

}
