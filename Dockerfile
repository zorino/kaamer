FROM golang:1.13.7-buster

RUN apt-get update \
    && curl -sL https://deb.nodesource.com/setup_12.x | bash - \
    && apt-get install -y nodejs

RUN mkdir /data
VOLUME [ "/data" ]

WORKDIR /go/src/app
COPY . .

RUN go install -v ./...

RUN cd web \
    && test -d node_modules && rm -fr node_modules \
    && npm install \
    && npm install -g gatsby \
    && gatsby build --prefix-paths

CMD ["kaamer-db", "-server", "-d", "/data"]
