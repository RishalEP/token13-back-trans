# Stage 1: Build the application
#FROM golang:1.24-alpine AS builder
FROM ${{ secrets.VM_HOST }}:5000/golang:1.24-alpine AS builder
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
ENV MYSQL_USER=${{ secrets.MYSQL_USER }} \
    MYSQL_PASS=${{ secrets.MYSQL_PASS }}\
    MYSQL_HOST=${{ secrets.MYSQL_HOST }} \
    MYSQL_PORT=${{ secrets.MYSQL_PORT }} \
    MYSQL_DBNAME=${{ secrets.MYSQL_DBNAME }} \
    REDIS_HOST=${{ secrets.REDIS_HOST }} \
    REDIS_PORT=${{ secrets.REDIS_PORT }}
CMD ["./back-trans"]
