// Copyright 2020 The SODA Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"github.com/micro/go-micro/v2"

	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	handler "github.com/opensds/multi-cloud/file/pkg/service"
	pb "github.com/opensds/multi-cloud/file/proto"
	log "github.com/sirupsen/logrus"
)

func main() {
	service := micro.NewService(
		micro.Name("file"),
	)

	obs.InitLogs()
	service.Init()

	log.Infof("Initializing File service..")

	pb.RegisterFileHandler(service.Server(), handler.NewFileService())
	if err := service.Run(); err != nil {
		log.Error(err)
	}
}
