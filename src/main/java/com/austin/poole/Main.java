package com.austin.poole;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.nio.charset.StandardCharsets;

import static com.austin.poole.Constants.getKinesisClient;

public class Main {

    public static void main(String[] args) {

        ShardUtil.printShardsDescription(Constants.STREAM_NAME);

        ShardUtil.printPartitionShardRange(Constants.STREAM_NAME, "Aang");
        ShardUtil.printPartitionShardRange(Constants.STREAM_NAME, "Katara");

        Shard s1 = ShardUtil.getStreamDescription(Constants.STREAM_NAME).streamDescription().shards().get(0);
        Shard s2 = ShardUtil.getStreamDescription(Constants.STREAM_NAME).streamDescription().shards().get(1);
        //Using a partition key
        sendMessage("message 1", "Aang",false);
        sendMessage("message 2", "Katara",false);


        sendMessage("message 3", s1.hashKeyRange().startingHashKey(),true);
        sendMessage("message 4", s2.hashKeyRange().startingHashKey(),true);
    }

    public static void sendMessage(String message, String key, boolean withExplicitHashKey) {
        KinesisClient kinesisClient = getKinesisClient();

        SdkBytes data = SdkBytes.fromByteArray(message.getBytes(StandardCharsets.UTF_8));
        PutRecordRequest.Builder putRecordRequestBuilder =
                PutRecordRequest.builder()
                        .partitionKey(key)
                        .streamName(Constants.STREAM_NAME)
                        .data(data);
        if (withExplicitHashKey) {
            putRecordRequestBuilder.explicitHashKey(key);  //overrides partition key
        }

        PutRecordResponse putRecordsResult = kinesisClient
                .putRecord(putRecordRequestBuilder.build());
        System.out.println("Put records Result" + putRecordsResult);
        kinesisClient.close();
    }

}
