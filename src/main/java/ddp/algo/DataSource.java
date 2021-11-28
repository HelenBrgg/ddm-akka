package ddp.algo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.util.stream.*;
import java.util.List;
import java.io.Reader;
import java.io.IOException;

public class DataSource {
    private List<String[]> entries;
    private int columnCount;

    public static class DataPoint {
        public String column;
        public String value;

        public DataPoint(String column, String value){
             this.column = column;
             this.value = value;
        }
    }

    public DataSource(Reader reader) throws IOException, CsvException {
        CSVReader csvReader = new CSVReader(reader);
        this.entries = csvReader.readAll();
        csvReader.close();
        assert(entries.size() >= 1);
        this.columnCount = this.entries.get(0).length;
    }

    private DataPoint getDataPoint(int idx){
        String col = this.entries.get(0)[idx % this.columnCount];
        String val = this.entries.get(idx / this.columnCount)[idx % this.columnCount];
        return new DataPoint(col, val);
    }

    public Stream<DataPoint> streamDataPoints(){
        return IntStream.range(this.columnCount, this.entries.size() * this.columnCount)
            .mapToObj(idx -> this.getDataPoint(idx));
    }
}
