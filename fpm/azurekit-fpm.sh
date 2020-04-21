#!/bin/bash

cd /data
for i in rpm deb; do
  echo $i
  fpm -s dir -t $i -a all -n "azurekit" -v "${VERSION}" \
  -m study@tenmax.io \
  --vendor TenMax \
  --license Apache \
  --description "CLI toolkit to interact with Microsoft Azure Blob Storage.
  There are several commands included.
  1. azruecat - Output a blob content to stdout.
  2. azuresink - Pipe stdin to a azure storage blob.
  3. azuresas - Generate the Shared-Access-Signature to stdout.
  4. azuretbl2csv - Dump azure table as csv file.
  5. azuretbl2json - Dump azure table as json file.

  For more details, please check https://github.com/tenmax/azurekit .
  " \
  --url https://github.com/tenmax/azurekit \
  "/data/azurekit/=/usr/share/azurekit" \
  /data/fpm/bin/=/usr/bin/
done
