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

package backend

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
	"github.com/micro/go-micro/client"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	backend "github.com/opensds/multi-cloud/backend/proto"
	dataflow "github.com/opensds/multi-cloud/dataflow/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

const (
	backendService  = "backend"
	s3Service       = "s3"
	dataflowService = "dataflow"
)

type APIService struct {
	backendClient  backend.BackendService
	s3Client       s3.S3Service
	dataflowClient dataflow.DataFlowService
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		backendClient:  backend.NewBackendService(backendService, c),
		s3Client:       s3.NewS3Service(s3Service, c),
		dataflowClient: dataflow.NewDataFlowService(dataflowService, c),
	}
}

func (s *APIService) GetBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:get") {
		return
	}
	log.Infof("Received request for backend details: %s\n", request.PathParameter("id"))
	id := request.PathParameter("id")
	ctx := context.Background()
	res, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: id})
	if err != nil {
		log.Errorf("failed to get backend details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	// do not return sensitive information
	res.Backend.Access = ""
	res.Backend.Security = ""

	log.Info("Get backend details successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) listBackendDefault(request *restful.Request, response *restful.Response) {
	listBackendRequest := &backend.ListBackendRequest{}

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Errorf("get pagination parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.Limit = limit
	listBackendRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Errorf("get sort parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.SortKeys = sortKeys
	listBackendRequest.SortDirs = sortDirs

	filterOpts := []string{"name", "type", "region"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Errorf("get filter failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.Filter = filter

	ctx := context.Background()
	res, err := s.backendClient.ListBackend(ctx, listBackendRequest)
	if err != nil {
		log.Errorf("failed to list backends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	// do not return sensitive information
	for _, v := range res.Backends {
		v.Access = ""
		v.Security = ""
	}

	log.Info("List backends successfully.")
	response.WriteEntity(res)
}

func (s *APIService) FilterBackendByTier(request *restful.Request, response *restful.Response, tier int32) {
	// Get those backend type which supporte the specific tier.
	req := pb.GetBackendTypeByTierRequest{Tier: tier}
	res, _ := s.s3Client.GetBackendTypeByTier(context.Background(), &req)
	req1 := &backend.ListBackendRequest{}
	resp := &backend.ListBackendResponse{}
	for _, v := range res.Types {
		// Get backends with specific backend type.
		filter := make(map[string]string)
		filter["type"] = v
		req1.Filter = filter
		res1, err := s.backendClient.ListBackend(context.Background(), req1)
		if err != nil {
			log.Errorf("failed to list backends of type[%s]: %v\n", v, err)
			response.WriteError(http.StatusInternalServerError, err)
		}
		if len(res1.Backends) != 0 {
			resp.Backends = append(resp.Backends, res1.Backends...)
		}
	}
	//TODO: Need to consider pagination

	// do not return sensitive information
	for _, v := range resp.Backends {
		v.Access = ""
		v.Security = ""
	}

	log.Info("fiterBackendByTier backends successfully.")
	response.WriteEntity(resp)
}

func (s *APIService) ListBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:list") {
		return
	}
	log.Info("Received request for backend list.")

	para := request.QueryParameter("tier")
	if para != "" { //List those backends which support the specific tier.
		tier, err := strconv.Atoi(para)
		if err != nil {
			log.Errorf("list backends with tier as filter, but tier[%s] is invalid\n", tier)
			response.WriteError(http.StatusBadRequest, errors.New("invalid tier"))
			return
		}
		s.FilterBackendByTier(request, response, int32(tier))
	} else {
		s.listBackendDefault(request, response)
	}
}

func (s *APIService) CreateBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:create") {
		return
	}
	log.Info("Received request for creating backend.")
	backendDetail := &backend.BackendDetail{}
	err := request.ReadEntity(&backendDetail)
	if err != nil {
		log.Errorf("failed to read request body: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ctx := context.Background()
	res, err := s.backendClient.CreateBackend(ctx, &backend.CreateBackendRequest{Backend: backendDetail})
	if err != nil {
		log.Errorf("failed to create backend: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Create backend successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) UpdateBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:update") {
		return
	}
	log.Infof("Received request for updating backend: %v\n", request.PathParameter("id"))
	updateBackendRequest := backend.UpdateBackendRequest{Id: request.PathParameter("id")}
	err := request.ReadEntity(&updateBackendRequest)
	if err != nil {
		log.Errorf("failed to read request body: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ctx := context.Background()
	res, err := s.backendClient.UpdateBackend(ctx, &updateBackendRequest)
	if err != nil {
		log.Errorf("failed to update backend: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Update backend successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) DeleteBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:delete") {
		return
	}
	id := request.PathParameter("id")
	log.Infof("Received request for deleting backend: %s\n", id)
	ctx := context.Background()
	owner := "test"
	res, err := s.s3Client.ListBuckets(ctx, &s3.BaseRequest{Id: owner})
	count := 0
	for _, v := range res.Buckets {
		res, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: id})
		if err != nil {
			log.Errorf("failed to get backend details: %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		backendname := res.Backend.Name
		if backendname == v.Backend {
			count++
		}
	}
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if count == 0 {
		_, err := s.backendClient.DeleteBackend(ctx, &backend.DeleteBackendRequest{Id: id})
		if err != nil {
			log.Errorf("failed to delete backend: %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Info("Delete backend successfully.")
		response.WriteHeader(http.StatusOK)
		return
	} else {
		log.Info("the backend can not be deleted, need to delete bucket first.")
		response.WriteError(http.StatusInternalServerError, BackendDeleteError.Error())
		return
	}
}

func (s *APIService) ListType(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "type:list") {
		return
	}
	log.Info("Received request for backend type list.")
	listTypeRequest := &backend.ListTypeRequest{}

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Errorf("get pagination parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTypeRequest.Limit = limit
	listTypeRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Errorf("get sort parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTypeRequest.SortKeys = sortKeys
	listTypeRequest.SortDirs = sortDirs

	filterOpts := []string{"name"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Errorf("get filter failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTypeRequest.Filter = filter

	ctx := context.Background()
	res, err := s.backendClient.ListType(ctx, listTypeRequest)
	if err != nil {
		log.Errorf("failed to list types: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("List types successfully.")
	response.WriteEntity(res)
}



