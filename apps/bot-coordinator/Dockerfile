# syntax=docker/dockerfile:1.6
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bot-coordinator ./apps/bot-coordinator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bot-coordinator /usr/local/bin/bot-coordinator
USER nonroot
ENTRYPOINT ["/usr/local/bin/bot-coordinator"]
