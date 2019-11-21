// Copyright 2019 The OpenSDS Authors.
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

package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketSSEDelete(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for deleting SSE on bucket: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.s3Client.DeleteBucketSSE(ctx, &s3.BaseRequest{Id: bucketName})
	log.Infof("rsp:%s, err:%v\n", rsp, err)
	if HandleS3Error(response, request, err, rsp.ErrorCode) != nil {
		log.Errorf("delete bucket[%s] SSE failed, err=%v, errCode=%d\n", bucketName, err, rsp.ErrorCode)
		return
	}

	log.Info("delete bucket SSE end.")
}
