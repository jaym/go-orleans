package examples

//go:generate protoc --proto_path=. --plugin=protoc-gen-goor=../../bin/protoc-gen-goor --go_out=. --go_opt=paths=source_relative --goor_out=. --goor_opt=paths=source_relative example.proto
