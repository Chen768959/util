package pers.cc.util.auto_producer.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Chen768959
 * @date 2024/1/17
 */
public class DorisHttpStreamRecordWriter implements RecordWriter {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String user;
    private final String password;
    private StringWriter writer;
    private CSVPrinter csvPrinter;
    private final String url;

    public DorisHttpStreamRecordWriter(String dbName, String tableName, String user, String password, String host, int port) throws IOException {
        this.url = "http://"+host+":"+port+"/api/"+dbName+"/"+tableName+"/_stream_load";
        this.user = user;
        this.password = password;
        initCsvWriter();
    }

    @Override
    public void write(List<Object> writableData) throws IOException {
        this.csvPrinter.printRecord(writableData);
    }

    @Override
    public void flush() throws IOException {
        try {
            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(600, TimeUnit.SECONDS)
                    .readTimeout(600, TimeUnit.SECONDS)
                    .writeTimeout(600, TimeUnit.SECONDS)
                    .build();

            Request request = new Request.Builder()
                    .url(url)
                    .put(RequestBody.create(MediaType.parse("text/plain"), this.writer.toString()))
                    .header("Authorization", Credentials.basic(user, password))
                    .header("column_separator", ",")
                    .build();

            Response response = client.newCall(request).execute();

            if (response.isSuccessful()) {
                if (response.body() != null){
                    String body = response.body().string();
                    JsonNode jsonNode = objectMapper.readTree(body);
                    if (jsonNode.get("Status") != null && "Success".equals(jsonNode.get("Status").asText())){
                        return;
                    }
                    throw new IOException("响应失败, res body：" + body);
                }
            }else {
                throw new IOException("响应失败, code：" + response.code() + ", msg" + response.message());
            }
        } finally {
            closeCsvWriter();
            initCsvWriter();
        }
    }

    @Override
    public void close() throws IOException {
        closeCsvWriter();
    }

    private void initCsvWriter() throws IOException {
        this.writer = new StringWriter();
        this.csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
    }

    private void closeCsvWriter() throws IOException {
        this.writer.close();
        this.csvPrinter.close();
    }
}
