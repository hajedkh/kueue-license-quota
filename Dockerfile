FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 go build -o /reconciler .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /reconciler /reconciler
USER nonroot:nonroot
ENTRYPOINT ["/reconciler"]
