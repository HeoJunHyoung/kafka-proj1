package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


// 피자 집 중에 특정 피자집은 대형 피자집이라서 따로 파티셔닝이 필요하다는 시나리오에 기반하여 작성
public class CustomPartitioner implements Partitioner {

    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKeyName;

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }

    /**
     * byte[] keyBytes, byte[] valueBytes
     * ㄴ Serialized된 byte code
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic); // Cluster cluset: Broker들의 정보 -> partition들의 정보 추출
        int numPartitions = partitionInfoList.size();   // partition 개수 추출 (5개)
        int numSpecialPartitions = (int) (numPartitions * (0.5)); // special topic에 대해서는 5개 중에 2개의 partition 할당 (2개)
        int partitionIndex = 0;


        if (keyBytes == null) {
//            return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidRecordException("key shoud not be null");
        }

        if (((String) key).equals(specialKeyName)) {
            // (keyBytes -> valueBytes)
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions; // 0,1 설정
        } else {
            // specialKey에 대한 파티셔닝이 0,1로 설정되었기 때문에 specialKey가 아닌 Key에 대해서는 2,3,4로 설정해줘야 함
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + 2;
        }

        logger.info("key:{} is sent to partition: {}", key.toString(), partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {
    }

}
