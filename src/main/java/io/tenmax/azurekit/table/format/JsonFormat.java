package io.tenmax.azurekit.table.format;

import com.google.gson.Gson;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class JsonFormat extends Format {
    Gson gson = new Gson();

    @Override
    public void head(String[] columns, boolean printHeader, PrintStream out) {

    }

    @Override
    public String map(DynamicTableEntity dynamicTableEntity) {
        Map<String, String> row = new HashMap<>();

        try {
            for (Map.Entry<String, EntityProperty> entry : dynamicTableEntity.getProperties().entrySet()) {
                row.put(entry.getKey(), entry.getValue().getValueAsString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return gson.toJson(row);
    }
}
