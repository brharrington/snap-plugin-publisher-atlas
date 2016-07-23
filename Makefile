
GLIDE = $(GOPATH)/bin/glide

all: build

$(GLIDE):
	curl https://glide.sh/get | sh

install: $(GLIDE)
	$(GOPATH)/bin/glide install

build: install
	go clean
	go build
	go test -v . ./atlas

linux: install
	env GOOS=linux GOARCH=amd64 go build
