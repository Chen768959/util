package pers.cc.util.auto_producer.producer.col_producer;

import pers.cc.util.auto_producer.ProduceDataConfig;

import java.sql.Types;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public class FloatFieldProducer extends IntFieldProducer{
    public FloatFieldProducer(ProduceDataConfig.ColRule colRule) {
        super(colRule);
    }

    @Override
    protected Object produceRandomField() {
        return (int)super.produceRandomField() + 0.1f;
    }

    @Override
    protected Object produceAutoIncField() {
        return (int)super.produceRandomField() + 0.1f;
    }


    @Override
    public int fieldType() {
        return Types.FLOAT;
    }
}
