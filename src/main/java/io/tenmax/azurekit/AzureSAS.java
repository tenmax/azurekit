package io.tenmax.azurekit;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import io.tenmax.azurekit.azure.AccountUtils;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AzureSAS {
    private static final int BUFFER_SIZE = 4 * 1024 * 1024;

    private CommandLine commandLine = null;
    private List<CloudStorageAccount> accounts = new ArrayList<CloudStorageAccount>();

    private void parseArgs(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption("c", true, "The connection string");
        options.addOption("e", true, "The seconds to expired. (default=86400s)");
        options.addOption("h", false, "The help information");
        options.addOption("v", false, "The version");

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
        System.out.println("azuresink version " + Consts.VERSION);
        System.exit(0);
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "azuresas [-c <connection-string>] -e <seconds> <blob-uri>";
        formatter.printHelp(cmdLineSyntax, options);
        System.exit(0);
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

    public AzureSAS(String[] args) {
        parseArgs(args);

        accounts = AccountUtils.readAccounts(commandLine.getOptionValue('c', ""));

        String path = commandLine.getArgs()[0];
        try {
            path = URLDecoder.decode(path, "utf-8");
        } catch (UnsupportedEncodingException e) {
            // don't use the decoded path. Use the original one
        }

        URI blobUri = URI.create(path);
        CloudStorageAccount account = AccountUtils.getAccountFromUri(accounts, blobUri);

        int duration = 86400;
        if (commandLine.hasOption("e")) {
            duration = Integer.parseInt(commandLine.getOptionValue("e"));
        }

        printSasUrl(account, blobUri, duration);
    }

    private void printSasUrl(CloudStorageAccount account, URI blobUri, int seconds) {

        try {
            String container = blobUri.getPath().split("/")[1];
            String path = blobUri.getPath().substring(2 + container.length());

            CloudBlobClient blobClient = account.createCloudBlobClient();
            CloudBlobContainer blobContainer = blobClient.getContainerReference(container);
            if (!blobContainer.exists()) {
                System.err.println("container not exists");
                return;
            }


            Iterable<ListBlobItem> blobs = blobContainer.listBlobs(path);

            for (ListBlobItem blobItem : blobs) {
                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;
                    if (blob.getUri().equals(blobUri)) {
                        printSasUrlByBlob(blob, seconds);
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void printSasUrlByBlob(CloudBlob blob, int seconds) throws StorageException, IOException, InvalidKeyException {
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setPermissionsFromString("r");
        policy.setSharedAccessExpiryTime(Date.from(Instant.now().plusSeconds(seconds)));

        String s = blob.generateSharedAccessSignature(policy, null);
        URI srcUri = URI.create(blob.getUri() + "?" + s);
        System.out.println(srcUri);
    }

    public static void main(String[] args) throws Exception {
        new AzureSAS(args);
    }
}


