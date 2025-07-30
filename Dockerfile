# Stage 1: Build the application
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o back-trans .

# Stage 2: Create the final image
FROM alpine:latest

# Install CA certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from the builder stage
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

# Run the application
CMD ["./back-trans"]
