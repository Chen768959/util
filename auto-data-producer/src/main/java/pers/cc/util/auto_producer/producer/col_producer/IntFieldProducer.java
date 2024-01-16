package pers.cc.util.auto_producer.producer.col_producer;

import pers.cc.util.auto_producer.ProduceDataConfig;

import java.sql.Types;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public class IntFieldProducer extends FieldProducerBase{
    private int autoIncInt = 0;
    int randomAdd = 0;

    public IntFieldProducer(ProduceDataConfig.ColRule colRule) {
        super(colRule);

        int nearLen = colRule.getNearLen();
        if (colRule.getNearLen() > Integer.toString(Integer.MAX_VALUE).length()){
            nearLen = Integer.toString(Integer.MAX_VALUE).length();
        }
        if (nearLen>0){
            /*
          nearLen(1)->randomAdd(1)
          nearLen(2)->randomAdd(10)
          nearLen(3)->randomAdd(100)
          ·····
         */
            randomAdd = (int) Math.pow(10, nearLen - 1);
            if (colRule.getRandomRange() >= (Integer.MAX_VALUE - randomAdd)){
                randomAdd = Integer.MAX_VALUE - colRule.getRandomRange();
            }
        }
    }

    @Override
    protected Object produceRandomField() {
        return randomAdd+random.nextInt(colRule.getRandomRange());
    }

    @Override
    protected Object produceAutoIncField() {
        autoIncInt++;
        if (autoIncInt == Integer.MAX_VALUE){
            autoIncInt = 1;
        }
        return autoIncInt;
    }

    @Override
    public int fieldType() {
        return Types.INTEGER;
    }
}
