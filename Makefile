build:
	go build ./cmd/client 

run: build
	./client
