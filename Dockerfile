FROM openjdk:8-jdk-slim as builder
ARG DOCKER_TAG=latest

COPY . /src
WORKDIR /src

RUN ./gradlew clean distTar -PprojVersion=${DOCKER_TAG}

FROM openjdk:8-jdk-slim
ARG DOCKER_TAG=latest

COPY --from=builder /src/build/distributions/azurekit-${DOCKER_TAG}.tar .

RUN tar xvf azurekit-${DOCKER_TAG}.tar -C /usr/share/ && \
    rm /usr/share/azurekit/bin/*.bat && \
    cp -rs /usr/share/azurekit/bin/* /usr/bin/ && \
    rm azurekit-${DOCKER_TAG}.tar