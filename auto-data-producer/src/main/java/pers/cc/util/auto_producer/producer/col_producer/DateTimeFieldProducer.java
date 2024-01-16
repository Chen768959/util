package pers.cc.util.auto_producer.producer.col_producer;

import pers.cc.util.auto_producer.ProduceDataConfig;
import pers.cc.util.auto_producer.context.Context;

import java.sql.Timestamp;
import java.sql.Types;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public class DateTimeFieldProducer extends FieldProducerBase{
    public DateTimeFieldProducer(ProduceDataConfig.ColRule colRule) {
        super(colRule);
    }

    @Override
    protected Object produceRandomField() {
        if (colRule.isDataTimeLatest()){
            return Context.getInstance().getCurrentTimestamp();
        }else {
            return new Timestamp(random.nextInt(colRule.getRandomRange()));
        }
    }

    @Override
    protected Object produceAutoIncField() {
        throw new IllegalArgumentException("DateTime not support auto increment");
    }

    @Override
    public int fieldType() {
        return Types.TIMESTAMP;
    }
}
