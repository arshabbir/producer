FROM golang



RUN mkdir /build

COPY . /build
COPY . /build

WORKDIR /build

RUN go mod download

RUN go build -o producer

EXPOSE 8082

ENTRYPOINT ["./producer"]

