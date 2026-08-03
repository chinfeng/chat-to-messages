# 中国国内镜像：基础镜像经 DaoCloud 镜像加速，Go 模块走 goproxy.cn，
# Alpine apk 源切换为阿里云镜像（避免海外源超时）。
FROM docker.m.daocloud.io/library/golang:1.26-alpine AS builder
WORKDIR /build
ENV GOPROXY=https://goproxy.cn,direct
COPY go.mod ./
COPY main.go ./
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o chat-to-messages .

FROM docker.m.daocloud.io/library/alpine:3.22
RUN sed -i 's#dl-cdn.alpinelinux.org#mirrors.aliyun.com#g' /etc/apk/repositories \
    && apk add --no-cache ca-certificates
COPY --from=builder /build/chat-to-messages /usr/local/bin/chat-to-messages
EXPOSE 8082
ENTRYPOINT ["chat-to-messages"]
