package io.tenmax.azurekit;

public class AzureTable2JSON {
    public static void main(String[] args) throws Exception {
        new AzureTableExporter(args, "azuretbl2json", "json");
    }
}
