package pers.cc.util.auto_producer.producer.col_producer;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public interface FieldProducer {
    Object produceField();

    /**
     * @return sqlType the SQL type code defined in <code>java.sql.Types</code>
     */
    int fieldType();
}
