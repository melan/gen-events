FROM golang:1.11 as build
ADD . /go/src/github.com/melan/gen-events/
WORKDIR /go/src/github.com/melan/gen-events/
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o /gen-events ./cmd/gen-events

FROM scratch
COPY --from=build /gen-events /
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["./gen-events"]