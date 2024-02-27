package com.austin.poole;

import jakarta.xml.bind.DatatypeConverter;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ShardUtil {

    public static void main(String[] args) {
        printPartitionShardRange(Constants.STREAM_NAME, "Bob's Burgers");
        printPartitionShardRange(Constants.STREAM_NAME, "Jimmy Pesto's Pizzeria");
    }
    public static Shard partitionShardRange(DescribeStreamResponse desc, String partitionKey) {

        //Wife made me remove streams for non-java devs 😠
//        Optional<Shard> shard = desc.streamDescription().shards().stream().filter(s->{
//                BigInteger startRange =  new BigInteger(s.hashKeyRange().startingHashKey(),16);
//                BigInteger endRange =  new BigInteger(s.hashKeyRange().endingHashKey(),16);
//                BigInteger val =  getMd5Hash(partitionKey);
//
//               return  startRange.compareTo(val) <= 0 && endRange.compareTo(val) >=0;
//        }).findFirst();

        Shard shardInRange = null;
        for (Shard s : desc.streamDescription().shards()) {
            BigInteger startRange = new BigInteger(s.hashKeyRange().startingHashKey(), 10);
            BigInteger endRange = new BigInteger(s.hashKeyRange().endingHashKey(), 10);
            BigInteger val = getMd5Hash(partitionKey);
            if(startRange.compareTo(val) <= 0 && endRange.compareTo(val) >=0){
                shardInRange = s;
            }
        }

        return shardInRange;
    }

    public static void printPartitionShardRange(String streamName, String partitionKey) {
        DescribeStreamResponse desc =getStreamDescription(streamName);
        Shard shard = partitionShardRange(desc,partitionKey);
        if(shard != null){
            System.out.println("Partition Key: "+ partitionKey);
            System.out.println("\tMD5: " + getMd5Hash(partitionKey));
            System.out.println("\tAssociated Shard ID : " + shard.shardId());
            System.out.println("\tStarting hash: " + shard.hashKeyRange().startingHashKey());
            System.out.println("\tEnding hash: " + shard.hashKeyRange().endingHashKey());
        }
    }

    public static DescribeStreamResponse getStreamDescription(String streamName){
        DescribeStreamResponse desc = Constants.getKinesisClient()
                .describeStream(DescribeStreamRequest.builder()
                        .streamName(streamName).build());
        return desc;
    }
        public static void printShardsDescription(String streamName) {
        DescribeStreamResponse desc =getStreamDescription(streamName);
        for (Shard s : desc.streamDescription().shards()) {
            System.out.println("Shard ID: " + s.shardId());
            System.out.println("\tStarting hash: " + s.hashKeyRange().startingHashKey());
            System.out.println("\tEnding hash: " + s.hashKeyRange().endingHashKey());
            System.out.println("-------------------------------------------------------");
        }
    }

    public static BigInteger getMd5Hash(String password) {

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.update(password.getBytes());
        byte[] digest = md.digest();
        String myHash = DatatypeConverter
                .printHexBinary(digest).toLowerCase();
        return new BigInteger(myHash, 16);

    }
}
