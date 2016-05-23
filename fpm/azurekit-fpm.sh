#!/bin/bash
cd /data
for i in rpm deb; do
fpm -s dir -t $i -a all -n azurekit -v 0.2.0 \
-m pop@tenmax.io \
--vendor tenmax \
--license Apache \
--description "CLI toolkit to interact with Microsoft Azure Blob Storage.
There are several commands included.
1. azruecat  - Output a blob content to stdout
2. azuresink - Pipe stdin to a azure storage blob.
3. azuresas  - Generate the Shared-Acess-Signature to stdout.
.
For more detial, please check https://github.com/tenmax/azurekit .
" \
--url https://github.com/tenmax/azurekit \
/data/azurekit/=/usr/share/azurekit \
/data/fpm/bin/=/usr/bin/
done
