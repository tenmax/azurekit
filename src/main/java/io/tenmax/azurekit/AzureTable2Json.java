package io.tenmax.azurekit;

import com.google.gson.Gson;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.*;
import org.apache.commons.cli.*;

import java.io.*;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by study on 6/21/16.
 */
public class AzureTable2Json {
    private CommandLine commandLine = null;
    private List<CloudStorageAccount> accounts = new ArrayList<>();

    private Gson gson = new Gson();

    private void parseArgs(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption("f", true, "The filter string");
        options.addOption("c", true, "The connection string");
        options.addOption("C", true, "The selected columns");
        options.addOption("H", "no-header-row", false, "Do not output column names.");

        try {
            // parse the command line arguments
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("Unexpected exception:" + e.getMessage());
            System.exit(1);
        }

        if (commandLine.hasOption('v')) {
            printVersion();
        } else if (commandLine.hasOption('h')) {
            printHelp(options);
        } else if (commandLine.getArgs().length != 1) {
            printHelp(options);
        }
    }

    private void printVersion() {
        System.out.println("azuretbl2csv version " + Consts.VERSION);
        System.exit(0);
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "azuretbl2csv [-c <connection-string>] -f filterString <table-url>";
        formatter.printHelp(cmdLineSyntax, options);
        System.exit(0);
    }

    public void readAccountsFromFile() {
        File file = new File(System.getProperty("user.home") + "/.azure/storagekeys");
        if (!file.exists()) {
            return;
        }

        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(file));
            String line;
            while ((line = in.readLine()) != null) {
                CloudStorageAccount account = CloudStorageAccount.parse(line);
                accounts.add(account);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void readAccountsFromArgs() {
        if (!commandLine.hasOption('c')) {
            return;
        }

        CloudStorageAccount account = null;
        try {
            account = CloudStorageAccount.parse(commandLine.getOptionValue('c'));
            accounts.add(account);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public AzureTable2Json(String[] args) {
        parseArgs(args);

        readAccountsFromFile();
        readAccountsFromArgs();

        String filterString = commandLine.getOptionValue("f");

        String[] columns = null;
        if (commandLine.hasOption("C")) {
            columns = commandLine.getOptionValue("C").split(",");
        }
        URI tableUri = null;

        String path = commandLine.getArgs()[0];
        try {
            path = URLDecoder.decode(path, "utf-8");
        } catch (UnsupportedEncodingException e) {
            // don't use the decoded path. Use the original one
        }

        tableUri = URI.create(path);

        String accountName = tableUri.getHost().split("\\.")[0];
        CloudStorageAccount account = null;
        for (CloudStorageAccount tmpAccount : accounts) {
            if (tmpAccount.getCredentials().getAccountName().equals(accountName)) {
                account = tmpAccount;
                break;
            }
        }
        if (account == null) {
            System.err.println("Connection String for " + accountName + " is not defined");
            System.exit(-1);
            return;
        }

        dump(account, tableUri, filterString, columns);
    }

    public void dump(CloudStorageAccount account, URI tableUri, String filterString, String[] columns) {
        try {
            PrintStream out = System.out;

            CloudTableClient tableClient = account.createCloudTableClient();
            String tableName = tableUri.getPath().substring(1);
            CloudTable cloudTable = tableClient.getTableReference(tableName);

            TableQuery<DynamicTableEntity> tb = new TableQuery<>();
            tb.setClazzType(DynamicTableEntity.class);
            tb.setFilterString(filterString);

            if (commandLine.hasOption("C")) {
                tb.setColumns(columns);
            }
            tb.setTakeCount(1);

            // Query
            for (DynamicTableEntity dynamicTableEntity : cloudTable.execute(tb)) {
                System.out.println(map(dynamicTableEntity));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    public static void main(String[] args) throws Exception {
        //args = new String[]{"-f (PartitionKey eq 'XXX')", "-C Origin,ApplicationName","https://ACCOUNT.table.core.windows.net/TABLE"};
        new AzureTable2Json(args);
    }
}
