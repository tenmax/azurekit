package io.tenmax.azurekit;

public class AzureTable2Csv {
    public static void main(String[] args) throws Exception {
        new AzureTableExporter(args, "azuretbl2csv", "csv");
    }
}
