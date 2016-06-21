package io.tenmax.azurekit;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import io.tenmax.azurekit.azure.AccountUtils;
import org.apache.commons.cli.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class AzureSink {
    private static final int BUFFER_SIZE = 4 * 1024 * 1024;

    private CommandLine commandLine = null;
    private List<CloudStorageAccount> accounts = new ArrayList<CloudStorageAccount>();

    private void parseArgs(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "c", true, "The connection string" );
        options.addOption( "f", false, "Force upload even the blob exists" );
        options.addOption( "h", false, "The help information" );
        options.addOption( "v", false, "The version" );

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
        System.out.println("azuresink version " + Consts.VERSION);
        System.exit(0);
    }

    private void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
                "azuresink [-c <connection-string>] <blob-uri>";
        formatter.printHelp(cmdLineSyntax, options);
        System.exit(0);
    }


    public AzureSink(String[] args) {
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

        upload(account, blobUri);
    }

    private void upload(CloudStorageAccount account, URI blobUri) {

        try {
            String container = blobUri.getPath().split("/")[1];
            String path = blobUri.getPath().substring(2 + container.length());

            CloudBlobClient blobClient = account.createCloudBlobClient();
            CloudBlobContainer blobContainer = blobClient.getContainerReference(container);
            if(!blobContainer.exists()) {
                System.err.println("container not exists");
                return;
            }


            Iterable<ListBlobItem> blobs = blobContainer.listBlobs(path);

            for (ListBlobItem blobItem : blobs) {
                if (blobItem instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) blobItem;
                    if (blob.getUri().equals(blobUri)) {
                        if (!commandLine.hasOption("f")) {
                            System.err.println("blob exists. Use -f to force upload.");
                            System.exit(-1);
                        }

                        blob.deleteIfExists();
                    }
                }
            }

            CloudBlockBlob blob = blobContainer.getBlockBlobReference(path);
            uploadBlob(System.in, blob);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void uploadBlob(InputStream  in, CloudBlockBlob blob) throws StorageException, IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int read;

        BlobOutputStream out = blob.openOutputStream();

        try {
            while ((read = in.read(buffer)) > 0) {
                out.write(buffer, 0, read);
            }
        } finally {
            in.close();
            out.close();
        }
    }

    public static void main(String[] args) throws Exception {
        new AzureSink(args);
    }
}


