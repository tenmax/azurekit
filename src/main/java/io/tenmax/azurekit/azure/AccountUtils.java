package io.tenmax.azurekit.azure;

import com.microsoft.azure.storage.CloudStorageAccount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AccountUtils {
    public static List<CloudStorageAccount> readAccounts(String connString) {
        List<CloudStorageAccount> accounts = readAccountsFromFile();
        readAccountsFromConnString(connString).ifPresent(accounts::add);

        return accounts;
    }

    private static List<CloudStorageAccount> readAccountsFromFile() {
        List<CloudStorageAccount> accounts = new ArrayList<>();

        File file = new File(System.getProperty("user.home") + "/.azure/storagekeys");
        if (!file.exists()) {
            return accounts;
        }

        try (BufferedReader in = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = in.readLine()) != null) {
                CloudStorageAccount account = CloudStorageAccount.parse(line);
                accounts.add(account);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return accounts;
    }

    private static Optional<CloudStorageAccount> readAccountsFromConnString(String connString) {
        Optional<CloudStorageAccount> account = Optional.empty();

        try {
            if (!connString.isEmpty()) {
                account = Optional.of(CloudStorageAccount.parse(connString));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        return account;
    }

    public static CloudStorageAccount getAccountFromUri(List<CloudStorageAccount> accounts, URI uri) {
        String accountName = uri.getHost().split("\\.")[0];

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
        }

        return account;
    }
}
