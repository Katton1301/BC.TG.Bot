FROM golang:1.21-bookworm AS builder

RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    gcc

WORKDIR /app
COPY go.mod go.sum computer_names.json ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /db-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    librdkafka++1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /db-server .
COPY --from=builder /app/.env .
COPY --from=builder /app/computer_names.json .

CMD ["./db-server"]