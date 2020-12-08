BINARY="rediscompare"
LINUX="linux-amd64"
DARWIN="darwin-adm64"
WIN="windows-amd64"
VERSION=1.0.0
BUILD=`date +%FT%T%z`

PACKAGES=`go list ./... | grep -v /vendor/`
VETPACKAGES=`go list ./... | grep -v /vendor/ | grep -v /examples/`
GOFILES=`find . -name "*.go" -type f -not -path "./vendor/*"`

default:
	@make clean
	@go mod tidy
	@go mod vendor
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-w -s' -o target/${BINARY}-${VERSION}-${LINUX}
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags '-w -s' -o target/${BINARY}-${VERSION}-${DARWIN}
	@CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags '-w -s' -o target/${BINARY}-${VERSION}-${WIN}.exe
	@echo "build finished,please check target directory"

linux:
	@make clean
	@go mod tidy
	@go mod vendor
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-w -s' -o target/${BINARY}-${VERSION}-${LINUX}

darwin:
	@make clean
	@go mod tidy
	@go mod vendor
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags '-w -s' -o target/${BINARY}-${VERSION}-${DARWIN}

windows:
	@make clean
	@go mod tidy
	@go mod vendor
	@CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags '-w -s' -o target/${BINARY}-${VERSION}-${WIN}.exe

list:
	@echo ${PACKAGES}
	@echo ${VETPACKAGES}
	@echo ${GOFILES}

fmt:
	@gofmt -s -w ${GOFILES}

test:
	@go test -cpu=1,2,4 -v -tags integration ./...

vet:
	@go vet $(VETPACKAGES)

docker:
    @docker build -t wuxiaoxiaoshen/example:latest .

clean:
	@if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
	@if [ -d ./target ] ; then rm -fr ./target ; fi

help:
	@echo "make - 格式化 Go 代码, 并编译生成二进制文件"
	@echo "make linux - 编译 Go 代码, 生成linux平台二进制文件"
	@echo "make darwin - 编译 Go 代码, 生成mac平台二进制文件"
	@echo "make windows - 编译 Go 代码, 生成windows平台二进制文件"
	@echo "make clean - 移除二进制文件和 vim swap files"


.PHONY: default linux darwin windows fmt test vet clean help
