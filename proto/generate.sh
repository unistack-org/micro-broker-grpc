#!/bin/sh -ex

PROTO_ARGS=" \
--proto_path=$(go list -f '{{ .Dir }}' -m github.com/envoyproxy/protoc-gen-validate) \
--proto_path=$(go list -f '{{ .Dir }}' -m go.unistack.org/micro-proto/v3) \
--proto_path=$(go list -f '{{ .Dir }}' -m go.unistack.org/protoc-gen-go-micro/v3@latest) \
--go_out=paths=source_relative:. \
--go-micro_out=module=go.unistack.org/micro-broker-grpc/v3,components=micro|grpc,standalone=true:. \
--validate_out=paths=source_relative,lang=go:."

export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin
export PATH=$(pwd)/bin:$PATH
export GOWORK=off
rm -rf proto *.pb.go *.pb.*.go
mkdir -p proto
protoc -I. $PROTO_ARGS ./*.proto