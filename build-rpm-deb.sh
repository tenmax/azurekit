#!/bin/bash
rm -f *.rpm *.deb
if [ -d azurekit ]; then
  rm -rf azurekit
fi
gradle clean
gradle build
tar xvf build/distributions/azurekit.tar
rm azurekit/bin/*.bat
docker run --rm --name fpm -h fpm -v $(pwd):/data tenzer/fpm /data/fpm/azurekit-fpm.sh
