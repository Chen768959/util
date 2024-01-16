package pers.cc.util.auto_producer.writer;

import java.sql.SQLException;
import java.util.List;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public interface RecordWriter {
    void write(List<Object> writableData) throws SQLException;

    void flush() throws SQLException;

    void close() throws SQLException;
}
