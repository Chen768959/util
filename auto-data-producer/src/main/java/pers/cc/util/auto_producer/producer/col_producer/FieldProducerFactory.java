package pers.cc.util.auto_producer.producer.col_producer;

import pers.cc.util.auto_producer.ProduceDataConfig;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public class FieldProducerFactory {
    public static FieldProducer createFieldProducer(ProduceDataConfig.ColRule colRule){
        switch (colRule.getDataType()){
            case "String":
                return new StringFieldProducer(colRule);
            case "DateTime":
                return new DateTimeFieldProducer(colRule);
            case "Int":
                return new IntFieldProducer(colRule);
            case "Float":
                return new FloatFieldProducer(colRule);
            case "Decimal":
                return new DecimalFieldProducer(colRule);
            default:
                throw new IllegalArgumentException("colRule dataType illegal, only be filled[String DateTime Int Float Decimal]");
        }
    }
}
