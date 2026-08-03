FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod ./
COPY main.go ./
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o chat-to-messages .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=builder /build/chat-to-messages /usr/local/bin/chat-to-messages
EXPOSE 8082
ENTRYPOINT ["chat-to-messages"]
