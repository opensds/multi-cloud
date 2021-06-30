# Copyright 2019 The OpenSDS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASE_DIR := $(shell pwd)
BUILD_DIR := $(BASE_DIR)/build
DIST_DIR := $(BASE_DIR)/build/dist
VERSION ?= $(shell git describe --exact-match 2> /dev/null || \
	     git describe --match=$(git rev-parse --short=8 HEAD) \
             --always --dirty --abbrev=8)
BUILD_TGT := soda-multicloud-$(VERSION)-linux-amd64

.PHONY: all build prebuild api aksk backend s3 dataflow block file docker clean

all: build

build: api aksk backend s3 dataflow datamover block file

prebuild:
	mkdir -p  $(BUILD_DIR)

api: prebuild
	CGO_ENABLED=0 GOOS=linux go build -ldflags 'GOFLAGS=-mod=vendor -w -s -extldflags "-static"' -o $(BUILD_DIR)/api github.com/opensds/multi-cloud/api/cmd

aksk: prebuild
	CGO_ENABLED=0 GOOS=linux go build -ldflags '-w -s -extldflags "-static"' -o $(BUILD_DIR)/aksk github.com/opensds/multi-cloud/aksk/cmd

backend: prebuild
	CGO_ENABLED=0 GOOS=linux go build -ldflags 'GOFLAGS=-mod=vendor -w -s -extldflags "-static"' -o $(BUILD_DIR)/backend github.com/opensds/multi-cloud/backend/cmd

s3: prebuild
	CGO_ENABLED=1 GOOS=linux go build -ldflags 'GOFLAGS=-mod=vendor -w -s -extldflags "-dynamic"' -o $(BUILD_DIR)/s3 github.com/opensds/multi-cloud/s3/cmd

dataflow: prebuild
	CGO_ENABLED=0 GOOS=linux go build -ldflags 'GOFLAGS=-mod=vendor -w -s -extldflags "-static"' -o $(BUILD_DIR)/dataflow github.com/opensds/multi-cloud/dataflow/cmd

datamover: prebuild
	CGO_ENABLED=0 GOOS=linux go build -ldflags 'GOFLAGS=-mod=vendor -w -s -extldflags "-static"' -o $(BUILD_DIR)/datamover github.com/opensds/multi-cloud/datamover/cmd

file: prebuild
	CGO_ENABLED=0 GOOS=linux go build -ldflags 'GOFLAGS=-mod=vendor -w -s -extldflags "-static"' -o $(BUILD_DIR)/file github.com/opensds/multi-cloud/file/cmd

block: prebuild
	CGO_ENABLED=0 GOOS=linux go build -ldflags 'GOFLAGS=-mod=vendor -w -s -extldflags "-static"' -o $(BUILD_DIR)/block github.com/opensds/multi-cloud/block/cmd

docker: build

	cp $(BUILD_DIR)/api api
	chmod 755 api/api
	docker build api -t sodafoundation/multi-cloud-api:latest

	cp $(BUILD_DIR)/aksk aksk
	chmod 755 aksk/aksk
	docker build aksk -t sodafoundation/multi-cloud-aksk:latest

	cp $(BUILD_DIR)/backend backend
	chmod 755 backend/backend
	docker build backend -t sodafoundation/multi-cloud-backend:latest

	cp $(BUILD_DIR)/file file
	chmod 755 file/file
	docker build file -t sodafoundation/multi-cloud-file:latest

	cp $(BUILD_DIR)/block block
	chmod 755 block/block
	docker build block -t sodafoundation/multi-cloud-block:latest

	cp $(BUILD_DIR)/s3 s3
	chmod 755 s3/s3
	chmod 755 s3/initdb.sh
	docker build s3 -t sodafoundation/multi-cloud-s3:latest

	cp $(BUILD_DIR)/dataflow dataflow
	chmod 755 dataflow/dataflow
	docker build dataflow -t sodafoundation/multi-cloud-dataflow:latest

	cp $(BUILD_DIR)/datamover datamover
	chmod 755 datamover/datamover
	docker build datamover -t sodafoundation/multi-cloud-datamover:latest

.PHONY: goimports
ifeq ($(CHECK_ERROR),y)
goimports: UNFORMATTED_FILES = $(shell goimports -local $(shell go list -m) -l $(shell go list -f {{.Dir}} ./... |grep -v /vendor/ | grep -v /proto))
goimports: 
	@ if [ ! -z "$(UNFORMATTED_FILES)" ]; then \
		echo "Formatting error in $(UNFORMATTED_FILES)"; \
		exit 1; \
	fi
else
goimports:
	goimports -local $(shell go list -m) -w $(shell go list -f {{.Dir}} ./... |grep -v /vendor/ | grep -v /proto)
endif

clean:
	rm -rf $(BUILD_DIR) api/api aksk/aksk backend/backend dataflow/dataflow datamover/datamover s3/s3 block/block file/file

version:
	@echo ${VERSION}

.PHONY: dist
dist: build
	rm -fr $(DIST_DIR) && mkdir -p $(DIST_DIR)/$(BUILD_TGT)/bin
	cd $(DIST_DIR) && \
	cp ../api $(BUILD_TGT)/bin/ && \
	cp ../aksk $(BUILD_TGT)/bin/ && \
	cp ../backend $(BUILD_TGT)/bin/ && \
	cp ../s3 $(BUILD_TGT)/bin/ && \
	cp ../dataflow $(BUILD_TGT)/bin/ && \
	cp ../datamover $(BUILD_TGT)/bin/ && \
	cp ../block $(BUILD_TGT)/bin/ && \
	cp ../file $(BUILD_TGT)/bin/ && \
	cp $(BASE_DIR)/LICENSE $(BUILD_TGT) && \
	zip -r $(DIST_DIR)/$(BUILD_TGT).zip $(BUILD_TGT) && \
	tar zcvf $(DIST_DIR)/$(BUILD_TGT).tar.gz $(BUILD_TGT)
	tree $(DIST_DIR)
