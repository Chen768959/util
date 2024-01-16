package pers.cc.util.auto_producer.producer.col_producer;

import pers.cc.util.auto_producer.ProduceDataConfig;

import java.math.BigDecimal;
import java.sql.Types;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public class DecimalFieldProducer extends IntFieldProducer{
    public DecimalFieldProducer(ProduceDataConfig.ColRule colRule) {
        super(colRule);
    }

    @Override
    protected Object produceRandomField() {
        return new BigDecimal(super.produceRandomField()+"."+random.nextInt(4));
    }

    @Override
    protected Object produceAutoIncField() {
        return new BigDecimal(super.produceAutoIncField()+"."+random.nextInt(4));
    }

    @Override
    public int fieldType() {
        return Types.DECIMAL;
    }
}
