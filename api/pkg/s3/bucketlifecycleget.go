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
	"fmt"
	"net/http"
	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/api/pkg/utils/constants"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
)

//Convert function from storage tier to storage class for XML format output
func (s *APIService) tier2class(tier int32) (string, error) {
	{
		mutext.Lock()
		defer mutext.Unlock()
		if len(ClassAndTier) == 0 {
			err := s.loadStorageClassDefinition()
			if err != nil {
				log.Errorf("load storage classes failed: %v.\n", err)
				return "", err
			}
		}
	}
	className := ""
	for k, v := range ClassAndTier {
		if v == tier {
			className = k
		}
	}
	if className == "" {
		log.Infof("invalid tier: %d\n", tier)
		return "", fmt.Errorf(InvalidTier)
	}
	return className, nil
}

//Function for GET Bucket Lifecycle API
func (s *APIService) BucketLifecycleGet(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for bucket details in GET lifecycle: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	bucket, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		log.Errorf("get bucket failed, err=%v\n", err)
		response.WriteError(http.StatusInternalServerError,
			fmt.Errorf("either the bucket does not exist or there is error when getting bucket details"))
	}

	// convert back to xml struct
	getLifecycleConf := model.LifecycleConfiguration{}

	// convert lifecycle rule to xml Rule
	if bucket.LifecycleConfiguration != nil {
		for _, lcRule := range bucket.LifecycleConfiguration {
			xmlRule := model.Rule{}

			xmlRule.Status = lcRule.Status
			xmlRule.ID = lcRule.Id
			xmlRule.Filter = converts3FilterToRuleFilter(lcRule.Filter)
			xmlRule.AbortIncompleteMultipartUpload = converts3UploadToRuleUpload(lcRule.AbortIncompleteMultipartUpload)
			xmlRule.Transition = make([]model.Transition, 0)

			//Arranging the transition and expiration actions in XML
			for _, action := range lcRule.Actions {
				log.Infof("action is : %v\n", action)

				if action.Name == ActionNameTransition {
					xmlTransition := model.Transition{}
					xmlTransition.Days = action.Days
					xmlTransition.Backend = action.Backend
					className, err := s.tier2class(action.Tier)
					if err == nil {
						xmlTransition.StorageClass = className
					}
					xmlRule.Transition = append(xmlRule.Transition, xmlTransition)
				}
				if action.Name == ActionNameExpiration {
					xmlExpiration := model.Expiration{}
					xmlExpiration.Days = action.Days
					xmlRule.Expiration = append(xmlRule.Expiration, xmlExpiration)
				}
			}
			// append each xml rule to xml array
			getLifecycleConf.Rule = append(getLifecycleConf.Rule, xmlRule)
		}
	}

	// marshall the array back to xml format
	response.WriteAsXml(getLifecycleConf)
	log.Info("GET lifecycle successful.")
}

func converts3FilterToRuleFilter(filter *s3.LifecycleFilter) model.Filter {
	retFilter := model.Filter{}
	if filter != nil {
		retFilter.Prefix = filter.Prefix
	}
	return retFilter
}

func converts3UploadToRuleUpload(upload *s3.AbortMultipartUpload) model.AbortIncompleteMultipartUpload {
	retUpload := model.AbortIncompleteMultipartUpload{}
	if upload != nil {
		retUpload.DaysAfterInitiation = upload.DaysAfterInitiation
	}
	return retUpload
}
