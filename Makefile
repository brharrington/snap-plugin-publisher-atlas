
build:
	go clean
	go build
	go test -v . ./atlas

linux:
	env GOOS=linux GOARCH=amd64 go build
