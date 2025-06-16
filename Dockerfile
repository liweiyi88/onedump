# syntax=docker/dockerfile:1

## Build
FROM golang:1.21.8-bookworm AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN apt-get update && apt-get upgrade -y && go mod download

COPY . ./

RUN go build -o /onedump

## Deploy
FROM gcr.io/distroless/base-debian12

WORKDIR /

COPY --from=build /onedump /onedump

ENTRYPOINT ["/onedump"]