package pers.cc.util.auto_producer.writer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Chen768959
 * @date 2024/1/11
 */
public interface RecordWriter {
    void write(List<Object> writableData) throws IOException;

    void flush() throws IOException;

    void close() throws IOException;
}
