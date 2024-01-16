package pers.cc.util.auto_producer.producer.col_producer;

import pers.cc.util.auto_producer.ProduceDataConfig;

import java.sql.Types;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public class StringFieldProducer extends FieldProducerBase{
    private int orderlyStringLen;
    private int autoIncInt = 0;
    private String autoIncRounds = "";

    public StringFieldProducer(ProduceDataConfig.ColRule colRule) {
        super(colRule);

        this.orderlyStringLen = colRule.getNearLen() - Integer.toString(colRule.getRandomRange()).length() - colRule.getStringPreRegular().length();
        if (orderlyStringLen<0){
            orderlyStringLen = 0;
        }
    }

    @Override
    protected Object produceRandomField() {
        return colRule.getStringPreRegular() + generateOrderlyString(orderlyStringLen) + random.nextInt(colRule.getRandomRange());
    }

    @Override
    protected Object produceAutoIncField() {
        autoIncInt++;
        if (autoIncInt == Integer.MAX_VALUE){
            autoIncInt = 1;
            autoIncRounds+="X";
        }
        return colRule.getStringPreRegular() + generateOrderlyString(orderlyStringLen) + autoIncRounds + autoIncInt;
    }


    @Override
    public int fieldType() {
        return Types.VARCHAR;
    }
}
