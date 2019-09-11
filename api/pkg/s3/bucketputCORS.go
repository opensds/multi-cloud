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
	"encoding/xml"
	"fmt"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/micro/go-micro/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
	"net/http"
	"strconv"
)

const (
	MaxCorsSize = 64 << 10 // 64 KB
)

func (s *APIService) BucketPutCORS(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Logf("Received request for CORS bucket: %s", bucketName)

	actx := request.Attribute(c.KContext).(*c.Context)
	ctx := metadata.NewContext(context.Background(), map[string]string{
		common.CTX_KEY_USER_ID:   actx.UserId,
		common.CTX_KEY_TENENT_ID: actx.TenantId,
		common.CTX_KEY_IS_ADMIN:  strconv.FormatBool(actx.IsAdmin),
	})
	contentLength := request.HeaderParameter("content-length")
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		log.Logf("get content length failed, err: %v\n", err)
		response.WriteError(http.StatusInternalServerError, InvalidContentLength.Error())
		return
	}
	if size > MaxCorsSize {
		response.WriteErrorString(http.StatusInternalServerError, "entity too large")
		return
	}

	bucket, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		log.Logf("get bucket failed, err=%v\n", err)
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("bucket does not exist"))
	}

	body := ReadBody(request)
	if body != nil {
		bucketcorsConf := model.Cors{}
		err := xml.Unmarshal(body, &bucketcorsConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		} else {
			s3CorsPtrArr := make([]*s3.CORSRule, 0)
			for _, rule := range bucketcorsConf.CorsRules {
				s3CORSRule := s3.CORSRule{}

				s3CORSRule.Id = rule.Id

				// Assigning the CORS configuration allowed methods
				s3CORSRule.AllowedMethods = rule.AllowedMethods

				// Assigning the CORS configuration allowed Origins
				s3CORSRule.AllowedOrigins = rule.AllowedOrigins

				// Assigning the CORS configuration allowed headers
				s3CORSRule.AllowedHeaders = rule.AllowedHeaders

				// Assigning the Maximum Age in seconds for CORS configuration
				s3CORSRule.MaxAgeSeconds = rule.MaxAgeSeconds

				// Assigning the CORS configuration Exposed headers
				s3CORSRule.ExposedHeaders = rule.ExposedHeaders

				s3CorsPtrArr = append(s3CorsPtrArr, &s3CORSRule)
			}

			// assign CORS rules to s3 bucket
			bucket.CorsConfiguration = s3CorsPtrArr
		}
	} else {
		log.Log("no request body provided for creating CORS configuration")
		response.WriteError(http.StatusBadRequest, fmt.Errorf(NoRequestBodyCORS))
		return
	}

	res, err := s.s3Client.UpdateBucket(ctx, bucket)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("Create bucket CORS successfully.")
	response.WriteEntity(res)
}
