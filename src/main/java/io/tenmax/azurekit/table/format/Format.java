package io.tenmax.azurekit.table.format;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.*;

import java.io.PrintStream;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

public abstract class Format {
    public abstract void head(String[] columns, boolean printHeader, PrintStream out);

    public abstract String map(DynamicTableEntity dynamicTableEntity);

    public void dump(CloudStorageAccount account, URI tableUri, String filterString, boolean printHeader, String[] columns, int takeCount, PrintStream os) {
        try {
            PrintStream out = System.out;

            CloudTableClient tableClient = account.createCloudTableClient();
            String tableName = tableUri.getPath().substring(1);
            CloudTable cloudTable = tableClient.getTableReference(tableName);

            TableQuery<DynamicTableEntity> tb = new TableQuery<>();
            tb.setClazzType(DynamicTableEntity.class);
            if(!filterString.isEmpty()) {
                tb.setFilterString(filterString);
            }

            tb.setColumns(columns);
            tb.setTakeCount(takeCount);

            // Query
            boolean isFirstCQL = true;
            for (DynamicTableEntity dynamicTableEntity : cloudTable.execute(tb)) {
                // Get the result set definitions.
                if (isFirstCQL) {
                    Map<String, EntityProperty> map = dynamicTableEntity.getProperties();
                    head(map.keySet().toArray(new String[map.size()]), printHeader, out);
                    isFirstCQL = false;
                }

                os.println(map(dynamicTableEntity));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
