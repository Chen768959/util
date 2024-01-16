package pers.cc.util.auto_producer.producer;

import java.util.List;

/**
 * @author Chen768959
 * @date 2024/1/10
 */
public interface DataProducer {
    List<Object> produceSingleData();

    List<Integer> dataTypeList();
}
