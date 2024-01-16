package pers.cc.util.auto_producer.producer;

import pers.cc.util.auto_producer.ProduceDataConfig;
import pers.cc.util.auto_producer.producer.col_producer.FieldProducer;
import pers.cc.util.auto_producer.producer.col_producer.FieldProducerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 非线程安全，应在单线程中使用
 * @author Chen768959
 * @date 2024/1/10
 */
public class DataProducerImpl implements DataProducer {
    private final List<FieldProducer> fieldProducerList;
    private final List<Integer> dataTypeList;

    public DataProducerImpl(ProduceDataConfig produceDataRule){
        this.fieldProducerList = produceDataRule.getColRules().stream()
                .map(FieldProducerFactory::createFieldProducer)
                .collect(Collectors.toList());

        this.dataTypeList = fieldProducerList.stream()
                .map(FieldProducer::fieldType)
                .collect(Collectors.toList());
    }

    @Override
    public List<Object> produceSingleData() {
        // 使用FieldProducer依次创建field
        return fieldProducerList.stream()
                .map(FieldProducer::produceField)
                .collect(Collectors.toList());
    }

    @Override
    public List<Integer> dataTypeList() {
        return this.dataTypeList;
    }
}
