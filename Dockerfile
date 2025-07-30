# Stage 1: Build the application
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o back-trans .
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/back-trans .
EXPOSE 8080
ENV MYSQL_USER=user \
    MYSQL_PASS=password \
    MYSQL_HOST=mysql \
    MYSQL_PORT=3306 \
    MYSQL_DBNAME=backtrans \
    REDIS_HOST=redis \
    REDIS_PORT=6379
CMD ["./back-trans"]