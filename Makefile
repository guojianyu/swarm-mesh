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
Author  := guojianyu

CLIENT_BINARIES := client
SERVER_BINARIES := server

DOCKER_CLIENT_NAME := mesh-client
DOCKER_SERVER_NAME := mesh-server

DOCKER_TAG  := v1.0

.PHONY: build
build:
	go build -o bin/$(CLIENT_BINARIES)  cmd/client.go
	go build -o bin/$(SERVER_BINARIES)  cmd/server.go

.PHONY: docker
docker:
	docker build -t  $(DOCKER_SERVER_NAME):$(DOCKER_TAG) -f docker/ServerDockerfile  .
	docker build -t  $(DOCKER_CLIENT_NAME):$(DOCKER_TAG) -f docker/ClientDockerfile  .

.PHONY: all
all:
	go build -o bin/$(CLIENT_BINARIES)  cmd/client.go
	go build -o bin/$(SERVER_BINARIES)  cmd/server.go
	docker build -t  $(DOCKER_SERVER_NAME):$(DOCKER_TAG) -f docker/ServerDockerfile  .
	docker build -t  $(DOCKER_CLIENT_NAME):$(DOCKER_TAG) -f docker/ClientDockerfile  .