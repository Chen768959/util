package pers.cc.util.auto_producer.producer.col_producer;

import pers.cc.util.auto_producer.ProduceDataConfig;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public abstract class FieldProducerBase implements FieldProducer{
    protected final ProduceDataConfig.ColRule colRule;
    protected final ThreadLocalRandom random = ThreadLocalRandom.current();

    public FieldProducerBase(ProduceDataConfig.ColRule colRule){

        this.colRule = colRule;
    }
    @Override
    public Object produceField() {
        if (colRule.isHasNull() && random.nextInt(100) == 0){
            return null;
        }

        if (colRule.isAutoInc()){
            return produceAutoIncField();
        }else {
            return produceRandomField();
        }
    }

    protected abstract Object produceRandomField();

    protected abstract Object produceAutoIncField();

    protected String generateOrderlyString(int length) {
        if (length <= 0) {
            return "";
        }

        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char ch = (char) ('A' + i % 26);
            stringBuilder.append(ch);
        }

        return stringBuilder.toString();
    }
}
