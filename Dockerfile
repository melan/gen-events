FROM golang:1.11 as build
RUN go get -u github.com/golang/dep/cmd/dep
WORKDIR /go
RUN go build github.com/golang/dep/cmd/dep

ADD . /go/src/github.com/melan/gen-events/
WORKDIR /go/src/github.com/melan/gen-events/
RUN /go/bin/dep ensure
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /gen-events ./cmd/gen-events

FROM scratch
COPY --from=build /gen-events /
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["./gen-events"]