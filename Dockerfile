FROM golang:1.11 as build

RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

RUN wget -O /cacert.pem https://curl.haxx.se/ca/cacert.pem

ADD . /go/src/github.com/melan/gen-events/
WORKDIR /go/src/github.com/melan/gen-events/
RUN /go/bin/dep ensure
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /gen-events ./cmd/gen-events

FROM scratch
COPY --from=build /gen-events /
COPY --from=build /cacert.pem /
WORKDIR /
EXPOSE 8080
ENV AWS_CA_BUNDLE /cacert.pem
ENTRYPOINT ["./gen-events"]