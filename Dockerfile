# FROM golang:1.24-alpine AS builder
# WORKDIR /app
# COPY go.mod go.sum ./
# RUN go mod download
# COPY . .
# RUN CGO_ENABLED=0 GOOS=linux go build -o back-trans .
# FROM alpine:latest
# RUN apk --no-cache add ca-certificates
# WORKDIR /app
# COPY --from=builder /app/back-trans .
# EXPOSE 8800
# CMD ["./back-trans"]

ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o back-trans .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/back-trans .

EXPOSE 8800
CMD ["./back-trans"]
