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

# Expose the application port
EXPOSE 8800

# Set environment variables (these will be overridden at runtime)
ENV MYSQL_USER=mysql \
    MYSQL_PASS=uWxATVsM9CS9m3Z\
    MYSQL_HOST=wallet_db \
    MYSQL_PORT=3306 \
    MYSQL_DBNAME=backtrans \
    REDIS_HOST=redis \
    REDIS_PORT=6379
CMD ["./back-trans"]