FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/uprest ./cmd/uprest \
    && mkdir -p /out/uprest-config

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/uprest /uprest
COPY --from=build /out/uprest-config /app/uprest-config
WORKDIR /app
ENV UPREST_PORT=8080
EXPOSE 8080
ENTRYPOINT ["/uprest"]
