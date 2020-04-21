#!/bin/bash

VERSION=$1

rm -f ./build/distributions/*.{rpm,deb}
if [ -d azurekit ]; then
  rm -rf azurekit
fi
tar --exclude=*.bat -xvf "build/distributions/azurekit-${VERSION}.tar"
docker run --rm --name fpm -h fpm -v "$(pwd):/data" -e "VERSION=${VERSION}" tenzer/fpm:no-entrypoint bash /data/fpm/azurekit-fpm.sh
rm -rf "azurekit"
mv azurekit*.{rpm,deb} build/distributions