# AzureKit
*azurekit* is a CLI toolkit to interact with [microsoft azure blob storage](https://azure.microsoft.com/en-us/documentation/articles/storage-introduction/#blob-storage). There are several commands included.

1. azruecat - Output a blob content to stdout.
2. azuresink - Pipe stdin to a azure storage blob.
3. azuresas - Generate the Shared-Access-Signature to stdout.
4. azuretbl2csv - Dump azure table as csv file.
5. azuretbl2json - Dump azure table as json file.


# Requirement
Java 8

# Getting Start

1. First, you must prepare your [Connection String](https://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/) for your storage account. The format is

	```bash
	DefaultEndpointsProtocol=[http|https];AccountName=myAccountName;AccountKey=myAccountKey
	```

2. Download from [release](https://github.com/tenmax/azurekit/releases).

3. Run the commands.

## AzureCat

Usage

1. Print a resource

	```bash
	azurecat https://<account-name>.blob.core.windows.net/<container-name>/<blob-path>
	```

2. Concatenate and print the resources with prefix

	```bash
	azurecat --prefix https://<account-name>.blob.core.windows.net/<container-name>/<blob-prefix>
	```
3. Concatenate and print the resources with prefix and the resource should match the postfix

	```bash
	azurecat --prefix --postfix csv https://<account-name>.blob.core.windows.net/<container-name>/<blob-prefix>
	```

4. Decode the resource by gzip compression format.

	```bash
	azurecat -z --prefix --postfix gz https://<account-name>.blob.core.windows.net/<container-name>/<blob-prefix>
	```

The full help for `azurecat`

```
usage: azurecat [-c <connection-string>] <blob-uri>
 -b <arg>                Set the read buffer size in KBytes
 -c <arg>                The connection string
 -h                      The help information
    --postfix <string>   keep only the blob which has the path with the
                         specified postfix. The postfix only be used while
                         prefix is used.
    --prefix             cat all the blobs with the prefix
 -v                      The version
 -z                      The gzip format
```


## AzureSink

Usage

1. Upload the content 'helloword' to the given path

```bash
echo 'helloworld' | azuresink -c <connection-string> http://<account-name>.blob.core.windows.net/<container-name>/<blob-path>
```

The full help for `azuresink`

```
usage: azuresink [-c <connection-string>] <blob-uri>
 -c <arg>   The connection string
 -f         Force upload even the blob exists
 -h         The help information
 -v         The version
```


## AzureSAS

Usage

1. Get the SAS url with 1 day expiration duration.

```bash
azuresas -c <connection-string> http://<account-name>.blob.core.windows.net/<container-name>/<blob-path>
```

The full help for `azuresas`

```
usage: azuresas [-c <connection-string>] -e <seconds> <blob-uri>
 -c <arg>   The connection string
 -e <arg>   The seconds to expired. (default=86400s)
 -h         The help information
 -v         The version
```

## AzureTable2CSV and AzureTable2JSON

Usage

1. Dump a table

	```bash
	azuretbl2csv https://<account-name>.table.core.windows.net/<table-name>
   azuretbl2json https://<account-name>.table.core.windows.net/<table-name>
	```

2. Select some columns

	```bash
	azuretbl2json -c "column1,column2" https://<account-name>.table.core.windows.net/<table-name>
	```
3. Apply filter

	```bash
	azuretbl2json -f "(PartitionKey eq 'pk1' and RowKey eq 'rk1')" https://<account-name>.table.core.windows.net/<table-name>
	```

The full help for `azuretbl2json` 

```
usage: azuretbl2json [-c <connection-string>] <table-url>
 -c <arg>             The connection string
 -C <arg>             The selected columns
 -f <arg>             The filter string
 -H,--no-header-row   Do not output column names.
 -t <arg>             The take count. Default=1000
```


# Configuration File

You can put your connection strings at `~/.azure/storagekeys` line by line

Here is the example.

```
DefaultEndpointsProtocol=https;AccountName=myAccountName1;AccountKey=myAccountKey1
DefaultEndpointsProtocol=https;AccountName=myAccountName2;AccountKey=myAccountKey2
```

then, you can use the resource directly without connection string specified.

```bash
echo 'helloworld' | azurecat https://<account-name>.blob.core.windows.net/<container-name>/<blob-path>
```

# Installation

## Mac

Install azurekit via [Homebrew](http://brew.sh/).

```bash
brew tap tenmax/azure
brew install azurekit
```

Upgrade azurekit

```bash
brew update
brew upgrade azurekit
```
