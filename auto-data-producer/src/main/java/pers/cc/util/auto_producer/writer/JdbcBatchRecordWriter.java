package pers.cc.util.auto_producer.writer;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.StringJoiner;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public class JdbcBatchRecordWriter implements RecordWriter{
    private final List<Integer> dataTypes;
    private final PreparedStatement statement;

    public JdbcBatchRecordWriter(String dbName, String tableName, List<String> colNameList, List<Integer> dataTypes, Connection connection) throws SQLException {
        this.dataTypes = dataTypes;
        this.statement = connection.prepareStatement(buildInsertSql(dbName, tableName, colNameList));
    }

    @Override
    public void write(List<Object> writableData) throws SQLException {
        for (int i = 0; i < dataTypes.size(); i++) {
            if (writableData.get(i) == null){
                this.statement.setNull(i+1, dataTypes.get(i));
            }else {
                switch (dataTypes.get(i)){
                    case Types.VARCHAR:
                        this.statement.setString(i + 1, (String) writableData.get(i));
                        break;
                    case Types.INTEGER:
                        this.statement.setInt(i + 1, (int)writableData.get(i));
                        break;
                    case Types.FLOAT:
                        this.statement.setFloat(i + 1, (float)writableData.get(i));
                        break;
                    case Types.DECIMAL:
                        this.statement.setBigDecimal(i + 1, (BigDecimal) writableData.get(i));
                        break;
                    case Types.TIMESTAMP:
                        this.statement.setTimestamp(i + 1, (Timestamp) writableData.get(i));
                        break;
                }
            }
        }
        this.statement.addBatch();
    }

    @Override
    public void flush() throws SQLException {
        this.statement.executeBatch();
        this.statement.clearBatch();
    }

    @Override
    public void close() throws SQLException {
        this.statement.close();
    }

    private String buildInsertSql(String dbName, String tableName, List<String> colNameList) {
        String columns = String.join(", ", colNameList);

        StringJoiner placeholders = new StringJoiner(", ");
        for (int i = 0; i < colNameList.size(); i++) {
            placeholders.add("?");
        }

        return "INSERT INTO " + dbName + "." + tableName + " (" + columns + ") VALUES (" + placeholders.toString() + ")";
    }
}
