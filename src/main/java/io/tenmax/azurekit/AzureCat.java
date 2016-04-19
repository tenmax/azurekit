package io.tenmax.azurekit;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import org.apache.commons.cli.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class AzureCat {
    private static final int READ_SIZE = 64 * 1024;

    private CommandLine commandLine = null;
    private List<CloudStorageAccount> accounts = new ArrayList<CloudStorageAccount>();

    private void parseArgs(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "b", true, "Set the read buffer size in KBytes" );
        options.addOption( "c", true, "The connection string" );
        options.addOption( "h", false, "The help information" );
        options.addOption( "v", false, "The version" );
        options.addOption( "z", false, "The gzip format" );
        options.addOption(Option.builder()
                .longOpt("prefix")
                .desc("cat all the blobs with the prefix")
                .build());
        options.addOption(Option.builder()
                .longOpt("postfix")
                .argName("string")
                .hasArg(true)
                .desc("keep only the blob which has the path with the specified postfix. The postfix only be used while prefix is used.")
                .build());


        try {
            // parse the command line arguments
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println( "Unexpected exception:" + e.getMessage() );
            System.exit(1);
        }

        if (commandLine.hasOption('v')) {
            printVersion();
        } else if(commandLine.hasOption('h')) {
            printHelp(options);
        } else if (commandLine.getArgs().length != 1) {
            printHelp(options);
        }
    }

    private void printVersion() {
        System.out.println("azurecat version " + Consts.VERSION);
        System.exit(0);
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "azurecat [-c <connection-string>] <blob-uri>";
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
            while ( (line = in.readLine()) != null) {
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
        if ( !commandLine.hasOption('c')) {
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

    public AzureCat(String[] args) {
        parseArgs(args);

        readAccountsFromFile();
        readAccountsFromArgs();

        boolean prefixMode = commandLine.hasOption("prefix");
        String postfix = commandLine.hasOption("postfix") ?
                commandLine.getOptionValue("postfix") :
                null;

        URI blobUri = null;


        String path = commandLine.getArgs()[0];
        try {
            path = URLDecoder.decode(path, "utf-8");
        } catch (UnsupportedEncodingException e) {
            // don't use the decoded path. Use the original one
        }

        blobUri = URI.create(path);

        String accountName = blobUri.getHost().split("\\.")[0];
        CloudStorageAccount account = null;
        for (CloudStorageAccount tmpAccount : accounts) {
            if(tmpAccount.getCredentials().getAccountName().equals(accountName)) {
                account = tmpAccount;
                break;
            }
        }
        if (account == null) {
            System.err.println("Connection String for " + accountName + " is not defined");
            System.exit(-1);
            return;
        }

        if (prefixMode) {
            catPrefix(account, blobUri, postfix);
        } else {
            catOne(account, blobUri);
        }
    }

    private void catOne(CloudStorageAccount account, URI blobUri) {

        try {
            String container = blobUri.getPath().split("/")[1];
            String path = blobUri.getPath().substring(2 + container.length());

            CloudBlobClient blobClient = account.createCloudBlobClient();
            CloudBlobContainer blobContainer = blobClient.getContainerReference(container);
            Iterable<ListBlobItem> blobs = blobContainer.listBlobs(path);

            boolean found = false;
            for (ListBlobItem blobItem : blobs) {
                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;
                    if (blob.getUri().equals(blobUri)) {
                        printBlob(blob);
                        return;
                    }
                }
            }

            System.err.println("Can't find blob at " + blobUri);
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void catPrefix(
            CloudStorageAccount account,
            URI blobUri,
            String postfix) {

        try {
            String container = blobUri.getPath().split("/")[1];
            String path = blobUri.getPath().substring(2 + container.length());

            CloudBlobClient blobClient = account.createCloudBlobClient();
            CloudBlobContainer blobContainer = blobClient.getContainerReference(container);
            Iterable<ListBlobItem> blobs = blobContainer.listBlobs(path);
            for (ListBlobItem blobItem : blobs) {

                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;

                    if (postfix != null &&
                            !blob.getUri().toString().endsWith(postfix)) {
                        continue;
                    }

                    try {
                        printBlob(blob);
                    } catch(Exception e) {
                        System.err.println("Can't print the blob at " + blob.getUri());
                        e.printStackTrace();
                        System.exit(-1);
                    }
                } else if (blobItem instanceof CloudBlobDirectory) {
                    catDirectory((CloudBlobDirectory) blobItem, postfix);
                }

                if (System.out.checkError()) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void catDirectory(CloudBlobDirectory directory,
                              String postfix)
            throws URISyntaxException, StorageException, IOException {
        Iterable<ListBlobItem> blobs = directory.listBlobs();
        for (ListBlobItem blobItem: blobs) {
            if (blobItem instanceof CloudBlob) {
                CloudBlob blob = (CloudBlob) blobItem;

                if(postfix != null &&
                        !blob.getUri().toString().endsWith(postfix))
                {
                    continue;
                }

                printBlob(blob);
            } else if (blobItem instanceof CloudBlobDirectory) {
                catDirectory((CloudBlobDirectory) blobItem, postfix);
            }
        }
    }



    private void printBlob(CloudBlob blob) throws StorageException, IOException {
        int readSize = READ_SIZE;

        if (commandLine.hasOption("b") ) {
            readSize = Constants.KB * Integer.parseInt(commandLine.getOptionValue("b"));
        }
        blob.setStreamMinimumReadSizeInBytes(readSize);
        InputStream in = blob.openInputStream();


        if(commandLine.hasOption("z")) {
            in = new GZIPInputStream(in);
        }

        try
        {
            byte[] buffer = new byte[readSize];
            int read;
            while ((read = in.read(buffer)) > 0) {
                System.out.write(buffer, 0, read);
                System.out.flush();

                if (System.out.checkError()) {
                    break;
                }
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new AzureCat(args);
    }
}


