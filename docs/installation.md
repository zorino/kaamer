# Getting started


## Installation

To build kaamer from source you will need at least GO >= 1.12 (https://golang.org/doc/install)

This command will download and install kaamer executables (kaamer and kaamer-db) into your `GOPATH`

``` shell
export GO111MODULE=off; go get -u github.com/zorino/kaamer
cd $GOPATH/src/github.com/zorino/kaamer
export GO111MODULE=on; go install ./...
```

> When issue https://github.com/golang/go/issues/31518 is resolved (~GO 1.13) 
> `go get -u github.com/zorino/kaamer/...` will suffice.

Or you can download precompiled binaries : https://github.com/zorino/kaamer/releases


## Requirement

Coming soon.. will be based on kaamer-bench and database size.
