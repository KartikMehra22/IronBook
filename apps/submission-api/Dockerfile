# syntax=docker/dockerfile:1.6
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/submission-api ./apps/submission-api

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/submission-api /usr/local/bin/submission-api
USER nonroot
ENTRYPOINT ["/usr/local/bin/submission-api"]
