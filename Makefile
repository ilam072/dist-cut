BINARY=dcut
SRC=cmd/main.go
.PHONY: build worker coord expected actual diff clean

build:
	go build -o $(BINARY) $(SRC)

clean:
	rm -f $(BINARY) expected.out actual.out