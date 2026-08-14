FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o secrets-manager-controller main.go

FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

RUN addgroup -S controllergroup && adduser -S controlleruser -G controllergroup
USER controlleruser

COPY --from=builder /app/secrets-manager-controller .

ENTRYPOINT ["/app/secrets-manager-controller"]