package io.tenmax.azurekit;

import com.microsoft.azure.storage.CloudStorageAccount;
import io.tenmax.azurekit.azure.AccountUtils;
import io.tenmax.azurekit.table.format.CsvFormat;
import io.tenmax.azurekit.table.format.Format;
import io.tenmax.azurekit.table.format.JsonFormat;
import org.apache.commons.cli.*;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.List;

public class AzureTableExporter {
    private CommandLine commandLine = null;
    private String appName;

    private void parseArgs(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption("c", true, "The connection string");
        options.addOption("f", true, "The filter string");
        options.addOption("C", true, "The selected columns");
        options.addOption("t", true, "The take count. Default=1000");
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
        System.out.println(appName + " version " + Consts.VERSION);
        System.exit(0);
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax = appName + " [-c <connection-string>] <table-url>";
        formatter.printHelp(cmdLineSyntax, options);
        System.exit(0);
    }

    public AzureTableExporter(String[] args, String appName, String fmt) {
        this.appName = appName;
        parseArgs(args);

        boolean printHeader = !commandLine.hasOption("H");
        List<CloudStorageAccount> accounts = AccountUtils.readAccounts(commandLine.getOptionValue('c', ""));
        String filterString = commandLine.getOptionValue("f", "");
        Format format = getFormat(fmt);
        Integer takeCount = Integer.valueOf(commandLine.getOptionValue("t", "1000"));
        String[] columns = null;
        if (commandLine.hasOption("C")) {
            columns = commandLine.getOptionValue("C").split(",");
        }

        String path = commandLine.getArgs()[0];
        try {
            path = URLDecoder.decode(path, "utf-8");
        } catch (UnsupportedEncodingException e) {
            // don't use the decoded path. Use the original one
        }

        URI tableUri = URI.create(path);
        CloudStorageAccount account = AccountUtils.getAccountFromUri(accounts, tableUri);

        format.dump(account, tableUri, filterString, printHeader, columns, takeCount, System.out);
    }

    public Format getFormat(String format) {
        format = format.toLowerCase();

        switch (format) {
            case "csv":
                return new CsvFormat();
            case "json":
            default:
                return new JsonFormat();
        }
    }
}
