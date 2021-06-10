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
	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature/credentials/keystonecredentials"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	"github.com/opensds/multi-cloud/api/pkg/utils/cryptography"
	backend "github.com/opensds/multi-cloud/backend/proto"
	dataflow "github.com/opensds/multi-cloud/dataflow/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"os"
	"io"
	"io/ioutil"
	"encoding/json"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	backendService_Docker  = "backend"
	s3Service_Docker       = "s3"
	dataflowService_Docker = "dataflow"
	backendService_K8S     = "soda.multicloud.v1.backend"
	s3Service_K8S          = "soda.multicloud.v1.s3"
	dataflowService_K8S    = "soda.multicloud.v1.dataflow"
)

// Map of object storage providers supported by s3 services. Keeping a map
// to optimize search
var objectStorage = map[string]int{
	"aws-s3":               1,
	"azure-blob":           1,
	"ibm-cos":              1,
	"hw-obs":               1,
	"ceph-s3":              1,
	"gcp-s3":               1,
	"fusionstorage-object": 1,
	"yig":                  1,
	"alibaba-oss":          1,
	"sony-oda":             1,
}

type APIService struct {
	backendClient  backend.BackendService
	s3Client       s3.S3Service
	dataflowClient dataflow.DataFlowService
}

type EnCrypter struct {
	Algo      string `json:"algo,omitempty"`
	Access    string `json:"access,omitempty"`
	PlainText string `json:"plaintext,omitempty"`
}

type DeCrypter struct {
	CipherText string `json:"ciphertext,omitempty"`
}

func isObjectStorage(storage string) bool {
	_, ok := objectStorage[storage]
	return ok
}

func NewAPIService(c client.Client) *APIService {

	backendService := backendService_Docker
	s3Service := s3Service_Docker
	dataflowService := dataflowService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		backendService = backendService_K8S
		s3Service = s3Service_K8S
		dataflowService = dataflowService_K8S
	}
	return &APIService{
		backendClient:  backend.NewBackendService(backendService, c),
		s3Client:       s3.NewS3Service(s3Service, c),
		dataflowClient: dataflow.NewDataFlowService(dataflowService, c),
	}
}


func ReadBody(r *restful.Request) []byte {
	var reader io.Reader = r.Request.Body
	b, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil
	}
	return b
}


func (s *APIService) GetBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:get") {
		return
	}
	log.Infof("Received request for backend details: %s\n", request.PathParameter("id"))
	id := request.PathParameter("id")

	ctx := common.InitCtxWithAuthInfo(request)
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

func (s *APIService) listBackendDefault(ctx context.Context, request *restful.Request, response *restful.Response) {
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

func (s *APIService) FilterBackendByTier(ctx context.Context, request *restful.Request, response *restful.Response,
	tier int32) {
	// Get those backend type which supporte the specific tier.
	req := s3.GetBackendTypeByTierRequest{Tier: tier}
	res, _ := s.s3Client.GetBackendTypeByTier(context.Background(), &req)
	req1 := &backend.ListBackendRequest{}
	resp := &backend.ListBackendResponse{}
	for _, v := range res.Types {
		// Get backends with specific backend type.
		filter := make(map[string]string)
		filter["type"] = v
		req1.Filter = filter
		res1, err := s.backendClient.ListBackend(ctx, req1)
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

	ctx := common.InitCtxWithAuthInfo(request)
	para := request.QueryParameter("tier")
	if para != "" { //List those backends which support the specific tier.
		tier, err := strconv.Atoi(para)
		if err != nil {
			log.Errorf("list backends with tier as filter, but tier[%v] is invalid\n", tier)
			response.WriteError(http.StatusBadRequest, errors.New("invalid tier"))
			return
		}
		s.FilterBackendByTier(ctx, request, response, int32(tier))
	} else {
		s.listBackendDefault(ctx, request, response)
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

	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	backendDetail.TenantId = actx.TenantId
	backendDetail.UserId = actx.UserId

	storageTypes, err := s.listStorageType(ctx, request, response)
	if err != nil {
		log.Errorf("failed to list backend storage type: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	foundType := typeExists(storageTypes.Types, backendDetail.Type)
	if !foundType {
		log.Errorf("failed to retrieve backend type: %v\n", err)
		response.WriteError(http.StatusBadRequest, err)
		return
	}

	backendDetailS3 := &s3.BackendDetailS3{}
	backendDetailS3.Id = backendDetail.Id
	backendDetailS3.Name = backendDetail.Name
	backendDetailS3.Type = backendDetail.Type
	backendDetailS3.Region = backendDetail.Region
	backendDetailS3.Endpoint = backendDetail.Endpoint
	backendDetailS3.BucketName = backendDetail.BucketName
	backendDetailS3.Access = backendDetail.Access
	backendDetailS3.Security = backendDetail.Security

	// This backend check will be called only for object storage
	if isObjectStorage(backendDetail.Type) {
		_, err = s.s3Client.BackendCheck(ctx, backendDetailS3)
		if err != nil {
			log.Errorf("failed to create backend due to wrong credentials: %v", err)
			err1 := errors.New("Failed to register backend due to invalid credentials.")
			response.WriteError(http.StatusBadRequest, err1)
			return
		}
	}

	res, err := s.backendClient.CreateBackend(ctx, &backend.CreateBackendRequest{Backend: backendDetail})
	if err != nil {
		log.Errorf("failed to create backend: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Created backend successfully.")
	response.WriteEntity(res.Backend)
}

func typeExists(slice []*backend.TypeDetail, inputType string) bool {
	for _, item := range slice {
		if item.Name == inputType {
			log.Debug("backend type is valid")
			return true
		}
	}
	return false
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

	ctx := common.InitCtxWithAuthInfo(request)
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

	ctx := common.InitCtxWithAuthInfo(request)
	// TODO: refactor this part
	res, err := s.s3Client.ListBuckets(ctx, &s3.BaseRequest{})
	count := 0
	for _, v := range res.Buckets {
		res, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: id})
		if err != nil {
			log.Errorf("failed to get backend details: %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		backendname := res.Backend.Name
		if backendname == v.DefaultLocation {
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
	log.Info("Received request for backends type list.")
	ctx := context.Background()
	storageTypes, err := s.listStorageType(ctx, request, response)
	if err != nil {
		log.Errorf("failed to list types of backend: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Info("List types successfully.")
	response.WriteEntity(storageTypes)
}

func (s *APIService) EncryptData(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:encrypt") {
		return
	}
	log.Info("Received request for encrypting data.")
	encrypter := &EnCrypter{}
	err := request.ReadEntity(&encrypter)
	if err != nil {
		log.Errorf("failed to read request body: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	credential, err := keystonecredentials.NewCredentialsClient(encrypter.Access).Get()
	if err != nil {
		log.Error(err)
		return
	}

	aes := cryptography.NewSymmetricKeyEncrypter(encrypter.Algo)
	cipherText, err := aes.Encrypter(encrypter.PlainText, []byte(credential.SecretAccessKey))
	if err != nil {
		log.Error(err)
		return
	}

	log.Info("Encrypt data successfully.")
	response.WriteEntity(DeCrypter{CipherText: cipherText})
}

func (s *APIService) listStorageType(ctx context.Context, request *restful.Request, response *restful.Response) (*backend.ListTypeResponse, error) {
	listTypeRequest := &backend.ListTypeRequest{}

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Errorf("get pagination parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return nil, err
	}
	listTypeRequest.Limit = limit
	listTypeRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Errorf("get sort parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return nil, err
	}
	listTypeRequest.SortKeys = sortKeys
	listTypeRequest.SortDirs = sortDirs

	filterOpts := []string{"name"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Errorf("get filter failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return nil, err
	}
	listTypeRequest.Filter = filter

	storageTypes, err := s.backendClient.ListType(ctx, listTypeRequest)
	return storageTypes, err
}

//tiering functions
func (s *APIService) CreateTier(request *restful.Request, response *restful.Response) {
	log.Info("Received request for creating tier.")
	tier := &backend.Tier{}
	body := ReadBody(request)
	err := json.Unmarshal(body, &tier)
	if err != nil{
	   log.Error("error occurred while decoding body", err)
	 response.WriteError(http.StatusBadRequest,nil)
	   return
	}
	if len(tier.Backends) == 0{
	   log.Error("tier can not be created as backends list is empty or correct" + " filed \"Backends\" is not present in request body")
	   response.WriteError(http.StatusBadRequest,err)
	   return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	tier.TenantId = actx.TenantId

	//validation of backends
	listBackendRequest := &backend.ListBackendRequest{}
	result, err := s.backendClient.ListBackend(ctx, listBackendRequest)
        if err != nil {
                log.Errorf("failed to list backends: %v\n", err)
                response.WriteError(http.StatusInternalServerError, err)
                return
        }

	var failedBackends []string
	for _,backendId := range tier.Backends{
		exists:=false
		for _,backend:= range result.Backends{
			if(backendId==backend.Id){
				exists=true;
				break;
			}
		}
		if exists==false{
		failedBackends=append(failedBackends,backendId)
	}
	}

	 if len(failedBackends) !=0{
                log.Errorf("failed to create tier due to the invalid backends:%v \n",failedBackends)
                response.WriteError(http.StatusBadRequest,err)
                return
        }

	res, err := s.backendClient.CreateTier(ctx, &backend.CreateTierRequest{Tier: tier})
	if err != nil {
		log.Errorf("failed to create tier: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Created backend successfully.")
	response.WriteEntity(res.Tier)

}

//here backendId can be updated
func (s *APIService) UpdateTier(request *restful.Request, response *restful.Response) {
	log.Infof("Received request for updating tier: %v\n", request.PathParameter("id"))
	
	id := request.PathParameter("id")
	updateTier := backend.UpdateTier{Id: id}
	body := ReadBody(request)
        err := json.Unmarshal(body, &updateTier)
        if err != nil{
           log.Error("error occurred while decoding body", err)
         response.WriteError(http.StatusBadRequest,nil)
           return
        }
        if len(updateTier.AddBackends)== 0 && len(updateTier.DeleteBackends)== 0{
           log.Error("tier can not be updated with empty addBackends and deleteBackends")
           response.WriteError(http.StatusBadRequest,err)
           return
        }


	ctx := common.InitCtxWithAuthInfo(request)

	res, err := s.backendClient.GetTier(ctx, &backend.GetTierRequest{Id: id})
        if err != nil {
                log.Errorf("failed to get tier details: %v\n", err)
                response.WriteError(http.StatusInternalServerError, err)
                return
        }

	//validation of add backends
	listBackendRequest := &backend.ListBackendRequest{}
        result, err := s.backendClient.ListBackend(ctx, listBackendRequest)
        if err != nil {
                log.Errorf("failed to list backends: %v\n", err)
                response.WriteError(http.StatusInternalServerError, err)
                return
        }

        var failBackends []string
        for _,backendId := range updateTier.AddBackends{
                exists:=false
                for _,backend:= range result.Backends{
                        if(backendId==backend.Id){
                                exists=true;
                                break;
                        }
                }
                if exists==false{
                failBackends=append(failBackends,backendId)
        }
        }
	var failDelBackends []string
        //check whether delete backends belong to tier
        for _,backendId:= range updateTier.DeleteBackends{
                found:=false
                for _,bcknd:= range res.Tier.Backends{
                        if(backendId==bcknd){
                                found=true
                        }
                }
                if found==false{
                        failDelBackends= append(failDelBackends,backendId)
                }
        }
        if len(failBackends)!=0 || len(failDelBackends)!=0 {
                err1 := errors.New("failed to update tier because backends are not proper")
		if len(failBackends)!=0{
			log.Errorf("cannot update tier because %v backends are not valid",failBackends)
		}
                if len(failDelBackends)!=0{
			log.Errorf("cannot update tier because %v backends are not present in tier",failDelBackends)
                }
		response.WriteError(http.StatusBadRequest,err1)
		return 
        }


        // backends to be deleted
        var delBackends []string
        for _,backendId:= range res.Tier.Backends{
                found:=false
                for _,bcknd:= range updateTier.DeleteBackends{
                        if(backendId==bcknd){
                                found=true
                        }
                }
                if found==false{
                        delBackends= append(delBackends,backendId)
                }
        }

        res.Tier.Backends= delBackends

	 //add backends to be added
        for _,backendId:= range updateTier.AddBackends{
                res.Tier.Backends= append(res.Tier.Backends,backendId)
        }

	updateTierRequest:= &backend.UpdateTierRequest{Tier: res.Tier}
	res1, err := s.backendClient.UpdateTier(ctx, updateTierRequest)
	if err != nil {
		log.Errorf("failed to update tier: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Update tier successfully.")
	response.WriteEntity(res1.Tier)
}

// GetTier if tierId is given then details of tier to be given
func (s *APIService) GetTier(request *restful.Request, response *restful.Response) {
	log.Infof("Received request for tier details: %s\n", request.PathParameter("id"))
	id := request.PathParameter("id")
	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.backendClient.GetTier(ctx, &backend.GetTierRequest{Id: id})
	if err != nil {
		log.Errorf("failed to get tier details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Get tier details successfully.")
	response.WriteEntity(res.Tier)
}

//List of tiers is displayed
func (s *APIService) ListTiers(request *restful.Request, response *restful.Response) {
	log.Info("Received request for tier list.")

	ctx := common.InitCtxWithAuthInfo(request)
	listTierRequest := &backend.ListTierRequest{}
	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Errorf("get pagination parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTierRequest.Limit = limit
	listTierRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Errorf("get sort parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTierRequest.SortKeys = sortKeys
	listTierRequest.SortDirs = sortDirs
	res, err := s.backendClient.ListTiers(ctx, listTierRequest)
	if err != nil {
		log.Errorf("failed to list backends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("List tiers successfully.")
	response.WriteEntity(res)
	return
}


//given tierId need to delete the tier
func (s *APIService) DeleteTier(request *restful.Request, response *restful.Response) {
	id := request.PathParameter("id")
	log.Infof("Received request for deleting tier: %s\n", id)
	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.backendClient.GetTier(ctx, &backend.GetTierRequest{Id: id})
	if err != nil {
		log.Errorf("failed to get tier details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	// check whether tier is empty
	if len(res.Tier.Backends) != 0 {
		log.Errorf("failed to delete tier because tier is not empty has backends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	_, err = s.backendClient.DeleteTier(ctx, &backend.DeleteTierRequest{Id: id})
	if err != nil {
		log.Errorf("failed to delete tier: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Info("Delete tier  successfully.")
	response.WriteHeader(http.StatusOK)
	return
}
