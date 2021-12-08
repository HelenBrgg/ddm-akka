package ddp.algo;

import com.opencsv.CSVReader;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import java.util.stream.*;
import java.util.List;
import java.io.Reader;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class DataSource {
    private String[] header;
    private List<String[]> content;

    @AllArgsConstructor
    @Getter
    public static class DataPoint {
        private String column;
        private String value;
    }

    private void initialize(String[] header, List<String[]> content) {
        for (String[] line: content) {
            assert(header.length == line.length);
        }
        this.header = header;
        this.content = content;
    }

    public DataSource(String[] header, List<String[]> content) {
        this.initialize(header, content);
    }

    public DataSource(Reader reader) throws IOException, CsvException {
        CSVReader csvReader = new CSVReaderBuilder(reader)
            .withCSVParser(new CSVParserBuilder()
                .withSeparator(';')
                .build())
            .build();
        List<String[]> entries = csvReader.readAll();
        csvReader.close();
        assert(!entries.isEmpty());
        String[] header = entries.remove(0);
        this.initialize(header, entries);
    }

    private DataPoint getDataPoint(int idx){
        String col = this.header[idx % this.header.length];
        String val = this.content.get(idx / this.header.length)[idx % this.header.length];
        return new DataPoint(col, val);
    }

    public Stream<DataPoint> streamDataPoints(){
        return IntStream.range(0, this.content.size() * this.header.length)
            .mapToObj(idx -> this.getDataPoint(idx));
    }
}
