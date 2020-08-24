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
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) ObjectDelete(request *restful.Request, response *restful.Response) {
	url := request.Request.URL
	bucketName := request.PathParameter("bucketName")
	objectName := request.PathParameter("objectKey")
	versionId := request.Request.URL.Query().Get(common.REQUEST_QUERY_VERSION_ID)
	if versionId != ""{
		objectName = objectName + "_" + versionId
	}


	if strings.HasSuffix(url.String(), "/") { // This is for folder.
		objectName = objectName + "/"
	}

	if len(bucketName) == 0 {
		log.Errorf("invalid input, bucket=%s\n", bucketName)
		WriteErrorResponse(response, request, ErrInvalidBucketName)
		return
	}
	if len(objectName) == 0 {
		log.Errorf("invalid input, object=%s\n", objectName)
		WriteErrorResponse(response, request, ErrInvalidObjectName)
	}

	input := s3.DeleteObjectInput{Bucket: bucketName, Key: objectName}
	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.s3Client.DeleteObject(ctx, &input)
	if HandleS3Error(response, request, err, rsp.GetErrorCode()) != nil {
		log.Errorf("delete object[%s] failed, err=%v, errCode=%d\n", objectName, err, rsp.GetErrorCode())
		return
	}

	if rsp.DeleteMarker {
		response.Header().Set("x-amz-delete-marker", "true")
	} else {
		response.Header().Set("x-amz-delete-marker", "false")
	}
	if rsp.VersionId != "" {
		response.Header().Set("x-amz-version-id", rsp.VersionId)
	}

	log.Infof("delete object[%s] from bucket[%s] succeed.", objectName, bucketName)
	WriteSuccessNoContent(response)
}
