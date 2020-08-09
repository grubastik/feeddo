FROM golang:alpine as builder
# kafka module require CGO
RUN apk add --no-cache git make build-base
RUN mkdir /build 
ADD . /build/
WORKDIR /build 
RUN go mod download
# for kafka to work properly we need to provide tag "must"
RUN CGO_ENABLED=1 go test -race -cover -tags musl ./...
RUN CGO_ENABLED=1 GOOS=linux go build -o feeddo -tags musl -ldflags '-extldflags "-static"' cmd/feeddo/main.go

FROM scratch
COPY --from=builder /build/feeddo /app/
WORKDIR /app
ENTRYPOINT ["./feeddo"]