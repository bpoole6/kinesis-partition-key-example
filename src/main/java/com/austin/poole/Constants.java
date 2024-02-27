package com.austin.poole;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class Constants {
    public static final String STREAM_NAME = "star_trek_is_better_than_star_wars";
    public static final Region REGION = Region.US_EAST_1;

    public static KinesisClient getKinesisClient() {
        return KinesisClient.builder().region(REGION).build();
    }
}
