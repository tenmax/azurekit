package io.tenmax.azurekit.table.format;

import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvFormat extends Format {

    @Override
    public void head(String[] columns, boolean printHeader, PrintStream out) {
        // Print the header
        if (printHeader) {
            List<String> list = Arrays.asList(columns).stream()
                    .collect(Collectors.toList());

            try {
                CSVPrinter print = CSVFormat.DEFAULT
                        .withHeader(list.toArray(new String[]{}))
                        .print(out);
                print.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String map(DynamicTableEntity dynamicTableEntity) {
        StringBuffer stringBuffer = new StringBuffer();
        try {
            CSVPrinter csvPrinter = new CSVPrinter(stringBuffer, CSVFormat.DEFAULT);
            for (Map.Entry<String, EntityProperty> entry : dynamicTableEntity.getProperties().entrySet()) {
                csvPrinter.print(entry.getValue().getValueAsString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return stringBuffer.toString();
    }
}
