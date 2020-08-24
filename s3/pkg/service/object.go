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

package service

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/micro/go-micro/v2/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/utils/constants"
	utils2 "github.com/opensds/multi-cloud/dataflow/pkg/utils"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/meta/types"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	meta "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	"github.com/opensds/multi-cloud/s3/pkg/meta/util"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

var ChunkSize int = 2048

const SecondsOneDay = 60 * 60 * 24

func (s *s3Service) CreateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Infoln("CreateObject is called in s3 service.")

	return nil
}

func (s *s3Service) UpdateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Infoln("PutObject is called in s3 service.")

	return nil
}

type DataStreamRecv interface {
	Recv() (*pb.PutDataStream, error)
}

type StreamReader struct {
	in   DataStreamRecv
	req  *pb.PutDataStream
	curr int
}

func (dr *StreamReader) Read(p []byte) (n int, err error) {
	left := len(p)
	for left > 0 {
		if dr.curr == 0 || (dr.req != nil && dr.curr == len(dr.req.Data)) {
			dr.req, err = dr.in.Recv()
			if err != nil && err != io.EOF {
				log.Errorln("failed to recv data with err:", err)
				return
			}
			if dr.req == nil || len(dr.req.Data) == 0 {
				log.Errorln("no data left to read.")
				err = io.EOF
				return
			}
			dr.curr = 0
		}

		copyLen := 0
		if len(dr.req.Data)-dr.curr > left {
			copyLen = left
		} else {
			copyLen = len(dr.req.Data) - dr.curr
		}
		log.Traceln("copy len:", copyLen)
		copy(p[n:], dr.req.Data[dr.curr:(dr.curr+copyLen)])
		dr.curr += copyLen
		left -= copyLen
		n += copyLen
	}
	return
}

func (s *s3Service) removeObjectFromBackend(ctx context.Context, sd driver.StorageDriver, obj *pb.DeleteObjectInput) error {
	if obj != nil {
		err := sd.Delete(ctx, obj)
		if err != nil {
			log.Errorln("failed to delete written object. err:", err)
			return err
		}
	}
	return nil
}

func (s *s3Service) PutObject(ctx context.Context, in pb.S3_PutObjectStream) error {
	log.Infoln("PutObject is called in s3 service.")

	var err error
	result := &pb.PutObjectResponse{}
	defer func() {
		result.ErrorCode = GetErrCode(err)
		in.SendMsg(result)
	}()

	isAdmin, tenantId, userId, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		log.Errorln("failed to get credential info. err:", err)
		return nil
	}

	req := &pb.PutObjectRequest{}
	err = in.RecvMsg(req)
	if err != nil {
		log.Errorln("failed to get msg with err:", err)
		return ErrInternalError
	}

	log.Infof("*********bucket:%s,key:%s,size:%d\n", req.BucketName, req.ObjectKey, req.Size)
	bucket, err := s.MetaStorage.GetBucket(ctx, req.BucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return err
	}

	if !isAdmin {
		switch bucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId {
				err = ErrBucketAccessForbidden
				return err
			}
		}
	}

	// get old object meta if it exist, this is not needed if versioning is enabled
	oldObj, err := s.MetaStorage.GetObject(ctx, bucket.Name, req.ObjectKey, "", false)
	if err != nil && err != ErrNoSuchKey {
		log.Errorf("get object[%s] failed, err:%v\n", req.ObjectKey, err)
		return ErrInternalError
	}
	log.Debugf("existObj=%v, err=%v\n", oldObj, err)

	data := &StreamReader{in: in}
	var limitedDataReader io.Reader
	if req.Size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, req.Size)
	} else {
		limitedDataReader = data
	}

	actualSize := req.Size

	// encrypt if needed
	obj := &pb.Object{BucketName: req.BucketName, ObjectKey: req.ObjectKey}
	// if the header contains the encryption header, encrypt
	headerValues, ok := req.Headers["X-Amz-Server-Side-Encryption"]
	if bucket.ServerSideEncryption.SseType == "SSE" {

		var readerErr error
		limitedDataReader, readerErr = utils.WrapEncryptionReader(io.LimitReader(data, req.Size),
			bucket.ServerSideEncryption.EncryptionKey,
			bucket.ServerSideEncryption.InitilizationVector)
		if readerErr != nil {
			log.Errorln("failed to get encrypted reader with err:", readerErr)
			return readerErr
		}
	} else if ok {
		encAlgo := headerValues.Values[0]
		if encAlgo == "AES256" {
			// encrypt
			byteArr, keyErr := utils.GetRandomNBitKey(ENC_KEY_LEN)
			if keyErr != nil {
				log.Error("Error generating SSE key", keyErr)
				return keyErr
			}
			ivArr, ivErr := utils.GetRandomNBitKey(ENC_IV_LEN)
			if ivErr != nil {
				log.Error("Error generating SSE IV", ivErr)
				return ivErr
			}
			obj.ServerSideEncryption = &pb.ServerSideEncryption{
				SseType:             "AES256",
				EncryptionKey:       byteArr,
				InitilizationVector: ivArr}

			var readerErr error
			limitedDataReader, readerErr = utils.WrapEncryptionReader(io.LimitReader(data, req.Size),
				obj.ServerSideEncryption.EncryptionKey,
				obj.ServerSideEncryption.InitilizationVector)
			if readerErr != nil {
				log.Errorln("failed to get encrypted reader with err:", readerErr)
				return readerErr
			}
		}
	}

	backendName := bucket.DefaultLocation
	if req.Location != "" {
		backendName = req.Location
	}
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, bucket.TenantId)
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	log.Infoln("bucket location:", req.Location, " backendtype:", backend.Type, " endpoint:", backend.Endpoint)
	bodyMd5 := req.Attrs["md5Sum"]
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_SIZE, req.Size)
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_MD5, bodyMd5)
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return err
	}

	if oldObj != nil && oldObj.Location == backendName {
		obj.StorageMeta = oldObj.StorageMeta
		obj.ObjectId = oldObj.ObjectId
	}
	res, err := sd.Put(ctx, limitedDataReader, obj)
	if err != nil {
		log.Errorln("failed to put data. err:", err)
		return err
	}

	obj.BucketName = req.BucketName
	obj.ObjectKey = req.ObjectKey
	obj.Acl = req.Acl
	obj.TenantId = tenantId
	obj.UserId = userId
	obj.ObjectId = res.ObjectId
	obj.LastModified = time.Now().UTC().Unix()
	obj.Etag = res.Etag
	obj.ContentType = req.ContentType
	obj.DeleteMarker = false
	obj.CustomAttributes = req.Attrs
	obj.Type = meta.ObjectTypeNormal
	obj.Tier = utils.Tier1 // Currently only support tier1
	obj.StorageMeta = res.Meta
	obj.EncSize = req.Size
	obj.Location = backendName
	if actualSize == -1 {
		obj.Size = res.Written
	} else {
		obj.Size = actualSize
	}

	object := &meta.Object{Object: obj}

	result.Md5 = res.Etag
	result.LastModified = object.LastModified

	err = s.MetaStorage.PutObject(ctx, object, oldObj, nil, nil, true)
	if err != nil {
		log.Errorf("failed to put object meta[object:%+v, oldObj:%+v]. err:%v\n", object, oldObj, err)
		// TODO: consistent check & clean
		return ErrDBError
	}

	return nil
}

func (s *s3Service) checkGetObjectRights(ctx context.Context, isAdmin bool, tenantId string, bucket *pb.Bucket, object *pb.Object) (err error) {
	if !isAdmin {
		switch object.Acl.CannedAcl {
		case "public-read", "public-read-write":
			break
		default:
			if object.TenantId != tenantId {
				err = ErrAccessDenied
				return
			}
		}
	}
	return
}

func (s *s3Service) GetObjectMeta(ctx context.Context, in *pb.GetObjectMetaRequest, out *pb.GetObjectMetaResult) error {
	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	bucket, err := s.MetaStorage.GetBucket(ctx, in.BucketName, true)
	if err != nil {
		log.Errorln("failed to get bucket from meta storage. err:", err)
		return nil
	}

	object, err := s.MetaStorage.GetObject(ctx, in.BucketName, in.ObjectKey, "", true)
	if err != nil {
		log.Errorln("failed to get object info from meta storage. err:", err)
		return nil
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return nil
	}

	err = s.checkGetObjectRights(ctx, isAdmin, tenantId, bucket.Bucket, object.Object)
	if err != nil {
		log.Errorln("failed to check source object rights. err:", err)
		return nil
	}

	// get expiration date
	if in.IsHeadReq == true {
		out.ExpireTime, out.RuleId = getObjectExpDate(bucket.LifecycleConfiguration, object.ObjectKey, object.LastModified)
	}

	out.Object = object.Object
	object.StorageClass, _ = GetNameFromTier(object.Tier, utils.OSTYPE_OPENSDS)
	return nil
}

func getObjectExpDate(lcRules []*pb.LifecycleRule, objKey string, lastModified int64) (int64, string) {
	var expTime int64 = 0
	for _, r := range lcRules {
		if r.Status == utils2.RuleStatusDisabled {
			continue
		}
		for _, ac := range r.Actions {
			if ac.Name == utils2.ActionNameExpiration {
				if r.Filter.Prefix == "" || strings.HasPrefix(objKey, r.Filter.Prefix) {
					exp := lastModified + SecondsOneDay*int64(ac.Days)
					if expTime < exp {
						expTime = exp
					}
					return expTime, r.Id
				}
			}
		}
	}

	return expTime, ""
}

func (s *s3Service) GetObject(ctx context.Context, req *pb.GetObjectInput, stream pb.S3_GetObjectStream) error {
	log.Infoln("GetObject is called in s3 service.")
	bucketName := req.Bucket
	objectName := req.Key
	offset := req.Offset
	length := req.Length

	var err error
	getObjRes := &pb.GetObjectResponse{}
	defer func() {
		getObjRes.ErrorCode = GetErrCode(err)
		stream.SendMsg(getObjRes)
	}()

	object, err := s.MetaStorage.GetObject(ctx, bucketName, objectName, "", true)
	if err != nil {
		log.Errorln("failed to get object info from meta storage. err:", err)
		return err
	}

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorln("failed to get bucket from meta storage. err:", err)
		return err
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil && isAdmin == false {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return nil
	}

	err = s.checkGetObjectRights(ctx, isAdmin, tenantId, bucket.Bucket, object.Object)
	if err != nil {
		log.Errorln("failed to check source object rights. err:", err)
		return err
	}

	backendName := bucket.DefaultLocation
	if object.Location != "" {
		backendName = object.Location
	}
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, bucket.TenantId)
	// if this object has only one part
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("unable to get backend. err:", err)
		return err
	}
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage driver. err:", err)
		return err
	}
	log.Infof("get object offset %v, length %v", offset, length)
	reader, err := sd.Get(ctx, object.Object, offset, offset+length-1)
	if err != nil {
		log.Errorln("failed to get data. err:", err)
		return err
	}

	var finalReader io.Reader
	finalReader = reader
	if bucket.ServerSideEncryption.SseType == "SSE" {
		finalReader, _ = utils.WrapAlignedEncryptionReader(reader, 0,
			bucket.ServerSideEncryption.EncryptionKey,
			bucket.ServerSideEncryption.InitilizationVector)
	} else if object.ServerSideEncryption != nil {
		if object.ServerSideEncryption.SseType == "AES256" {
			finalReader, _ = utils.WrapAlignedEncryptionReader(reader, 0,
				object.ServerSideEncryption.EncryptionKey,
				object.ServerSideEncryption.InitilizationVector)
		}
	}

	eof := false
	left := object.Size
	buf := make([]byte, ChunkSize)
	for !eof && left > 0 {
		n, err := finalReader.Read(buf)
		if err != nil && err != io.EOF {
			log.Errorln("failed to read, err:", err)
			break
		}
		// From https://golang.org/pkg/io/, a Reader returning a non-zero number of bytes at the end of the input stream
		// may return either err == EOF or err == nil. The next Read should return 0, EOF.
		// If err is equal to io.EOF, a non-zero number of bytes may be returned.
		if err == io.EOF {
			log.Debugln("finished read")
			eof = true
		}
		// From https://golang.org/pkg/io/, there is the following statement.
		// Implementations of Read are discouraged from returning a zero byte count with a nil error, except when len(p) ==
		// 0. Callers should treat a return of 0 and nil as indicating that nothing happened; in particular it does not indicate EOF.
		// If n is equal 0, it indicate that there is no more data to read
		if n == 0 {
			log.Infoln("reader return zero bytes.")
			break
		}

		err = stream.Send(&pb.GetObjectResponse{ErrorCode: int32(ErrNoErr), Data: buf[0:n]})
		if err != nil {
			log.Infof("stream send error: %v\n", err)
			break
		}
		left -= int64(n)
	}

	log.Infof("get object end, object:%s, left bytes:%d, err:%v\n", objectName, left, err)
	return err
}

func (s *s3Service) UpdateObjectMeta(ctx context.Context, in *pb.Object, out *pb.PutObjectResponse) error {
	log.Infoln("UpdateObjectMeta is called in s3 service.")
	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	object, err := s.MetaStorage.GetObject(ctx, in.BucketName, in.ObjectKey, "", true)
	if err != nil {
		log.Errorln("failed to get object info from meta storage. err:", err)
		return err
	}
	_, _, _, err = CheckRights(ctx, object.TenantId)
	if err != nil {
		log.Errorln("failed to check rights, err:", err)
		return err
	}

	err = s.MetaStorage.UpdateObjectMeta(&meta.Object{Object: in})
	if err != nil {
		log.Errorf("failed to update object meta storage, err:", err)
		err = ErrInternalError
		return err
	}
	out.LastModified = in.LastModified
	out.Md5 = in.Etag
	out.VersionId = in.GetVersionId()

	return nil
}

func (s *s3Service) CopyObject(ctx context.Context, in *pb.CopyObjectRequest, out *pb.CopyObjectResponse) error {
	log.Infoln("CopyObject is called in s3 service.")
	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	srcBucketName := in.SrcBucketName
	srcObjectName := in.SrcObjectName
	targetBucketName := in.TargetBucketName
	targetObjectName := in.TargetObjectName
	targetBucket, err := s.MetaStorage.GetBucket(ctx, targetBucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return nil
	}

	isAdmin, tenantId, userId, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		log.Errorf("get credential faied, err:%v\n", err)
		return nil
	}

	if !isAdmin {
		switch targetBucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if targetBucket.TenantId != tenantId {
				err = ErrBucketAccessForbidden
				return nil
			}
		}
	}

	srcBucket, err := s.MetaStorage.GetBucket(ctx, srcBucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return nil
	}
	srcObject, err := s.MetaStorage.GetObject(ctx, srcBucketName, srcObjectName, "", true)
	if err != nil {
		log.Errorln("failed to get object info from meta storage. err:", err)
		return nil
	}

	err = s.checkGetObjectRights(ctx, isAdmin, tenantId, srcBucket.Bucket, srcObject.Object)
	if err != nil {
		log.Errorln("failed to check source object rights. err:", err)
		return nil
	}

	backendName := srcBucket.DefaultLocation
	if srcObject.Location != "" {
		backendName = srcObject.Location
	}
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, srcBucket.TenantId)
	srcBackend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return nil
	}
	srcSd, err := driver.CreateStorageDriver(srcBackend.Type, srcBackend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return nil
	}

	targetBackendName := targetBucket.DefaultLocation
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, targetBucket.TenantId)
	targetBackend, err := utils.GetBackend(ctx, s.backendClient, targetBackendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return nil
	}
	// get old object meta if it exist
	oldObj, err := s.MetaStorage.GetObject(ctx, targetBucketName, targetObjectName, "", false)
	if err != nil && err != ErrNoSuchKey {
		log.Errorf("get object[%s] failed, err:%v\n", targetObjectName, err)
		err = ErrInternalError
		return nil
	}
	log.Debugf("existObj=%v, err=%v\n", oldObj, err)
	targetSd, err := driver.CreateStorageDriver(targetBackend.Type, targetBackend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return nil
	}
	log.Debugf("***ctx:%+v\n", ctx)
	reader, err := srcSd.Get(ctx, srcObject.Object, 0, srcObject.Size-1)
	if err != nil {
		log.Errorln("failed to put data. err:", err)
		return nil
	}
	limitedDataReader := io.LimitReader(reader, srcObject.Size)

	targetObject := &pb.Object{
		ObjectKey:  targetObjectName,
		BucketName: targetBucketName,
		Size:       srcObject.Size,
	}
	if oldObj != nil && oldObj.Location == targetBackendName {
		targetObject.StorageMeta = oldObj.StorageMeta
		targetObject.ObjectId = oldObj.ObjectId
	}
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_SIZE, srcObject.Size)
	res, err := targetSd.Put(ctx, limitedDataReader, targetObject)
	if err != nil {
		log.Errorln("failed to put data. err:", err)
		return nil
	}
	if res.Written < srcObject.Size {
		// TODO: delete incomplete object at backend
		log.Warnf("write objects, already written(%d), total size(%d)\n", res.Written, srcObject.Size)
		err = ErrIncompleteBody
		return nil
	}

	targetObject.Etag = res.Etag
	targetObject.ObjectId = res.ObjectId
	targetObject.LastModified = time.Now().UTC().Unix()
	targetObject.ContentType = srcObject.ContentType
	targetObject.DeleteMarker = false
	targetObject.Type = meta.ObjectTypeNormal
	targetObject.StorageMeta = res.Meta
	targetObject.Location = targetBackendName
	targetObject.TenantId = tenantId
	targetObject.UserId = userId
	// this is the default acl setting
	if in.Acl != nil {
		targetObject.Acl = in.Acl
	} else {
		targetObject.Acl = targetBucket.Acl
	}
	// we only support copy data with sse but not support copy data without sse right now
	targetObject.ServerSideEncryption = srcObject.ServerSideEncryption
	if validTier(in.TargetTier) {
		targetObject.Tier = in.TargetTier
	} else {
		targetObject.Tier = srcObject.Tier
	}
	if len(in.CustomAttributes) != 0 {
		targetObject.CustomAttributes = in.CustomAttributes
	} else {
		targetObject.CustomAttributes = srcObject.CustomAttributes
	}

	err = s.MetaStorage.PutObject(ctx, &meta.Object{Object: targetObject}, oldObj, nil, nil, true)
	if err != nil {
		log.Errorf("failed to put object meta[object:%+v, oldObj:%+v]. err:%v\n", targetObject, oldObj, err)
		// TODO: consistent check & clean
		err = ErrDBError
		return nil
	}

	out.Md5 = res.Etag
	out.LastModified = targetObject.LastModified

	log.Infoln("Successfully copy object ", res.Written, " bytes.")
	return nil
}

func initTargeObject(ctx context.Context, in *pb.MoveObjectRequest, srcObject *pb.Object) (*pb.Object, error) {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		log.Error("get metadata from ctx failed.")
		return nil, ErrInternalError
	}

	targetObject := &pb.Object{
		ObjectKey:            srcObject.ObjectKey,
		BucketName:           srcObject.BucketName,
		ObjectId:             srcObject.ObjectId,
		Size:                 srcObject.Size,
		Etag:                 srcObject.Etag,
		Location:             srcObject.Location,
		Tier:                 srcObject.Tier,
		TenantId:             srcObject.TenantId,
		UserId:               srcObject.UserId,
		StorageMeta:          srcObject.StorageMeta,
		LastModified:         srcObject.LastModified,
		ContentType:          srcObject.ContentType,
		ServerSideEncryption: srcObject.ServerSideEncryption,
		Acl:                  srcObject.Acl,
		Type:                 srcObject.Type,
		DeleteMarker:         false,
		CustomAttributes:     md, /* TODO: only reserve http header attr*/
	}

	if in.TargetTier > 0 {
		targetObject.Tier = in.TargetTier
	}

	return targetObject, nil
}

// This is for lifecycle management.
func (s *s3Service) MoveObject(ctx context.Context, in *pb.MoveObjectRequest, out *pb.MoveObjectResponse) error {
	log.Infoln("MoveObject is called in s3 service.")

	err := s.checkMoveRequest(ctx, in)
	if err != nil {
		return err
	}

	srcObject, err := s.MetaStorage.GetObject(ctx, in.SrcBucket, in.SrcObject, in.SrcObjectVersion, true)
	if err != nil {
		log.Errorf("failed to get object[%s] of bucket[%s]. err:%v\n", in.SrcObject, in.SrcBucket, err)
		return err
	}

	targetObject, err := initTargeObject(ctx, in, srcObject.Object)
	if err != nil {
		log.Errorf("failed to get init target obejct. err:%v\n", err)
		return err
	}

	var srcSd, targetSd driver.StorageDriver
	var srcBucket, targetBucket *types.Bucket
	srcBucket, err = s.MetaStorage.GetBucket(ctx, in.SrcBucket, true)
	if err != nil {
		log.Errorf("get source bucket[%s] failed with err:%v", in.SrcBucket, err)
		return err
	}

	srcBackend, err := utils.GetBackend(ctx, s.backendClient, srcObject.Location)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}
	srcSd, err = driver.CreateStorageDriver(srcBackend.Type, srcBackend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return err
	}

	if in.MoveType == utils.MoveType_ChangeStorageTier {
		log.Infof("chagne storage class of %s\n", targetObject.ObjectKey)
		// just change storage tier
		targetBucket = srcBucket
		className, err := GetNameFromTier(in.TargetTier, srcBackend.Type)
		if err != nil {
			return ErrInternalError
		}
		err = srcSd.ChangeStorageClass(ctx, targetObject, &className)
		if err != nil {
			log.Errorf("change storage class of object[%s] failed, err:%v\n", targetObject.ObjectKey, err)
			return err
		}
		newObj := &meta.Object{Object: targetObject}
		err = s.MetaStorage.UpdateObject4Lifecycle(ctx, srcObject, newObj, nil)
	} else {
		// need move data, get target location first
		if in.MoveType == utils.MoveType_ChangeLocation {
			targetBucket = srcBucket
			targetObject.Location = in.TargetLocation
			log.Infof("move %s cross backends, srcBackend=%s, targetBackend=%s, targetTier=%d\n",
				srcObject.ObjectKey, srcObject.Location, targetObject.Location, targetObject.Tier)
		} else { // MoveType_MoveCrossBuckets
			log.Infof("move %s from bucket[%s] to bucket[%s]\n", targetObject.ObjectKey, srcObject.BucketName,
				targetObject.BucketName)
			targetBucket, err = s.MetaStorage.GetBucket(ctx, in.TargetBucket, true)
			if err != nil {
				log.Errorf("get bucket[%s] failed with err:%v\n", in.TargetBucket, err)
				return err
			}
			targetObject.ObjectKey = in.TargetObject
			targetObject.Location = targetBucket.DefaultLocation
			targetObject.BucketName = targetBucket.Name
			log.Infof("move %s cross buckets, targetBucket=%s, targetBackend=%s, targetTier=%d\n",
				srcObject.ObjectKey, targetObject.BucketName, targetObject.Location, targetObject.Tier)
		}

		// get storage driver
		targetBackend, err := utils.GetBackend(ctx, s.backendClient, targetObject.Location)
		if err != nil {
			log.Errorln("failed to get backend client with err:", err)
			return err
		}
		targetSd, err = driver.CreateStorageDriver(targetBackend.Type, targetBackend)
		if err != nil {
			log.Errorln("failed to create storage. err:", err)
			return err
		}

		// copy data from one backend to another
		err = s.copyData(ctx, srcSd, targetSd, srcObject.Object, targetObject)
		if err != nil {
			log.Errorf("failed to copy object[%s], err:%v", srcObject.ObjectKey, err)
			return err
		}
		newObj := &meta.Object{Object: targetObject}
		if srcObject.Etag != targetObject.Etag {
			log.Errorf("data integrity check failed, etag of source object is %s, etag of target object is:%s\n",
				srcObject.Etag, targetObject.Etag)
			// if failed, delete target object
			s.cleanObject(ctx, newObj, targetSd)
			return err
		}
		out.Md5 = targetObject.Etag
		out.LastModified = targetObject.LastModified
		// update object meta data
		err = s.MetaStorage.UpdateObject4Lifecycle(ctx, srcObject, newObj, nil)
		if err != nil {
			log.Errorln("failed to update meta data after copy, err:", err)
			// if failed, delete target object
			s.cleanObject(ctx, newObj, targetSd)
			return err
		}
		// delete source object
		s.cleanObject(ctx, srcObject, srcSd)
		log.Infof("delete source object[key=%s]\n", srcObject.ObjectKey)
	}

	log.Infoln("MoveObject is finished.")

	return nil
}

func (s *s3Service) copyData(ctx context.Context, srcSd, targetSd driver.StorageDriver, srcObj, targetObj *pb.Object) error {
	log.Infof("copy object data")
	reader, err := srcSd.Get(ctx, srcObj, 0, srcObj.Size-1)
	if err != nil {
		log.Errorln("failed to get data. err:", err)
		return err
	}
	limitedDataReader := io.LimitReader(reader, srcObj.Size)
	res, err := targetSd.Put(ctx, limitedDataReader, targetObj)
	if err != nil {
		log.Errorln("failed to put data. err:", err)
		return err
	}
	log.Infoln("Successfully copy ", res.Written, " bytes.")

	targetObj.Etag = res.Etag
	targetObj.ObjectId = res.ObjectId
	targetObj.StorageMeta = res.Meta

	return nil
}

func (s *s3Service) checkMoveRequest(ctx context.Context, in *pb.MoveObjectRequest) (err error) {
	log.Infoln("check copy request")

	if in.SrcBucket == "" || in.SrcObject == "" {
		log.Errorf("invalid copy source.")
		err = ErrInvalidCopySource
		return
	}

	switch in.MoveType {
	case utils.MoveType_ChangeStorageTier:
		if !validTier(in.TargetTier) {
			log.Error("cannot copy object to it's self.")
			err = ErrInvalidCopyDest
		}
	case utils.MoveType_ChangeLocation:
		if in.TargetLocation == "" {
			log.Errorf("no target lcoation provided for change location copy")
			err = ErrInvalidCopyDest
		}
		// in.TargetTier > 0 means need to change storage class
		if in.TargetTier > 0 {
			if !validTier(in.TargetTier) {
				log.Error("cannot copy object to it's self.")
				err = ErrInvalidCopyDest
			}
		}
	case utils.MoveType_MoveCrossBuckets:
	default:
		// copy cross buckets as default
		if in.TargetObject == "" || in.TargetBucket == "" {
			log.Errorf("invalid copy target")
			err = ErrInvalidCopyDest
			return
		}
		// in.TargetTier > 0 means need to change storage class
		if in.TargetTier > 0 {
			if !validTier(in.TargetTier) {
				log.Error("cannot copy object to it's self.")
				err = ErrInvalidCopyDest
			}
		}
	}

	log.Infof("MoveType:%d, srcObject:%v\n", in.MoveType, in.SrcObject)
	return
}

// When bucket versioning is Disabled/Enabled/Suspended, and request versionId is set/unset:
//
// |           |        with versionId        |                   without versionId                    |
// |-----------|------------------------------|--------------------------------------------------------|
// | Disabled  | error                        | remove object                                          |
// | Enabled   | remove corresponding version | add a delete marker                                    |
// | Suspended | remove corresponding version | remove null version object(if exists) and add a        |
// |           |                              | null version delete marker                             |
//
// See http://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html
func (s *s3Service) DeleteObject(ctx context.Context, in *pb.DeleteObjectInput, out *pb.DeleteObjectOutput) error {
	log.Infoln("DeleteObject is called in s3 service, bucket:%s, key:%s, version:%s", in.Bucket, in.Key, in.VersioId)

	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	bucket, err := s.MetaStorage.GetBucket(ctx, in.Bucket, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return nil
	}

	object, err := s.MetaStorage.GetObject(ctx, in.Bucket, in.Key, in.VersioId, true)
	if err != nil {
		log.Errorln("failed to get object info from meta storage. err:", err)
		return nil
	}
	isAdmin, tenantId, _, err := CheckRights(ctx, object.TenantId)
	if err != nil {
		log.Errorf("no rights to access the object[%s]\n", object.ObjectKey)
		return nil
	}

	// administrator can delete any resource
	if isAdmin == false {
		switch bucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId && tenantId != "" {
				log.Errorf("delete object failed: tenant[id=%s] has no access right.", tenantId)
				err = ErrBucketAccessForbidden
				return nil
			}
		} // TODO policy and fancy ACL
	}

	switch bucket.Versioning.Status {
	case utils.VersioningDisabled:
		err = s.removeObject(ctx, bucket, object, tenantId)
	case utils.VersioningEnabled:
		err = s.removeObject(ctx, bucket, object)
	case utils.VersioningSuspended:
		// TODO: versioning
		err = ErrInternalError
	default:
		log.Errorf("versioing of bucket[%s] is invalid:%s\n", bucket.Name, bucket.Versioning)
		err = ErrInternalError
	}

	// TODO: need to refresh cache if it is enabled

	return nil
}

func (s *s3Service) removeObject(ctx context.Context, bucket *meta.Bucket, obj *Object, requestTenant string) error {
	if obj == nil {
		log.Infof("no need remove")
		return nil
	}
	log.Infof("remove object[%s] from bucket[%s]\n", obj.ObjectKey, bucket.Name)
	backendName := bucket.DefaultLocation
	if obj.Location != "" {
		backendName = obj.Location
	}
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, requestTenant, bucket.TenantId)
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend with err:", err)
		return err
	}
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage, err:", err)
		return err
	}

	// delete object data in backend
	err = sd.Delete(ctx, &pb.DeleteObjectInput{Bucket: bucket.Name, Key: obj.ObjectKey, VersioId: obj.VersionId,
		ETag: obj.Etag, StorageMeta: obj.StorageMeta, ObjectId: obj.ObjectId})
	if err != nil {
		log.Errorf("failed to delete obejct[%s,versionid=%s] from backend storage, err:%v\n", obj.ObjectKey, obj.VersionId, err)
		return err
	} else {
		log.Infof("delete obejct[%s,versionid=%s] from backend storage successfully.\n", obj.ObjectKey, obj.VersionId)
	}

	// delete object meta data from database
	err = s.MetaStorage.DeleteObject(ctx, obj)
	if err != nil {
		log.Errorf("failed to delete obejct[key=%s,versionid=%s] metadata, err:%v", obj.ObjectKey, obj.VersionId, err)
	} else {
		log.Infof("delete obejct[key=%s,versionid=%s] metadata successfully.", obj.ObjectKey, obj.VersionId)
	}

	return err
}

func (s *s3Service) ListObjects(ctx context.Context, in *pb.ListObjectsRequest, out *pb.ListObjectsResponse) error {
	log.Infof("ListObject is called in s3 service, bucket is %s.\n", in.Bucket)
	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()
	// Check ACL
	bucket, err := s.MetaStorage.GetBucket(ctx, in.Bucket, true)
	if err != nil {
		log.Errorf("err:%v\n", err)
		return nil
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return nil
	}

	// administrator can get any resource
	if isAdmin == false {
		switch bucket.Acl.CannedAcl {
		case "public-read", "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId {
				log.Errorf("tenantId(%s) does not much bucket.TenantId(%s)", tenantId, bucket.TenantId)
				err = ErrBucketAccessForbidden
				return nil
			}
		}
		// TODO validate user policy and ACL
	}

	// this is for aws s3 compatible
	if in.MaxKeys == 0 {
		out.IsTruncated = false
		return nil
	}

	retObjects, appendInfo, err := s.ListObjectsInternal(ctx, in)
	if appendInfo.Truncated && len(appendInfo.NextMarker) != 0 {
		out.NextMarker = appendInfo.NextMarker
	}
	if in.Version == constants.ListObjectsType2Int {
		out.NextMarker = util.Encrypt(out.NextMarker)
	}

	objects := make([]*pb.Object, 0, len(retObjects))
	for _, obj := range retObjects {
		object := pb.Object{
			LastModified:     obj.LastModified,
			Etag:             "\"" + obj.Etag + "\"",
			Size:             obj.Size,
			Tier:             obj.Tier,
			Location:         obj.Location,
			TenantId:         obj.TenantId,
			BucketName:       obj.BucketName,
			VersionId:        obj.VersionId,
			CustomAttributes: obj.CustomAttributes,
			ContentType:      obj.ContentType,
			StorageMeta:      obj.StorageMeta,
			ObjectKey:        obj.ObjectKey,
		}
		object.StorageClass, _ = GetNameFromTier(obj.Tier, utils.OSTYPE_OPENSDS)
		objects = append(objects, &object)
		log.Debugf("object:%+v\n", object)
	}
	out.Objects = objects
	out.Prefixes = appendInfo.Prefixes
	out.IsTruncated = appendInfo.Truncated

	err = ErrNoErr
	return nil
}

func (s *s3Service) ListObjectsInternal(ctx context.Context, request *pb.ListObjectsRequest) (retObjects []*meta.Object,
	appendInfo utils.ListObjsAppendInfo, err error) {
	log.Infoln("Prefix:", request.Prefix, "Marker:", request.Marker, "MaxKeys:",
		request.MaxKeys, "Delimiter:", request.Delimiter, "Version:", request.Version,
		"keyMarker:", request.KeyMarker, "versionIdMarker:", request.VersionIdMarker)

	filt := make(map[string]string)
	if request.Versioned {
		filt[common.KMarker] = request.KeyMarker
		filt[common.KVerMarker] = request.VersionIdMarker
	} else if request.Version == constants.ListObjectsType2Int {
		if request.ContinuationToken != "" {
			var marker string
			marker, err = util.Decrypt(request.ContinuationToken)
			if err != nil {
				err = ErrInvalidContinuationToken
				return
			}
			filt[common.KMarker] = marker
		} else {
			filt[common.KMarker] = request.StartAfter
		}
	} else { // version 1
		filt[common.KMarker] = request.Marker
	}

	filt[common.KPrefix] = request.Prefix
	filt[common.KDelimiter] = request.Delimiter
	// currentlly, request.Filter only support filter by 'lastmodified' and 'tier'
	for k, v := range request.Filter {
		filt[k] = v
	}

	return s.MetaStorage.Db.ListObjects(ctx, request.Bucket, request.Versioned, int(request.MaxKeys), filt)
}

func (s *s3Service) cleanObject(ctx context.Context, object *Object, sd driver.StorageDriver) error {
	delInput := &pb.DeleteObjectInput{
		Bucket: object.BucketName, Key: object.ObjectKey, ObjectId: object.ObjectId,
		VersioId: object.VersionId, StorageMeta: object.StorageMeta,
	}
	var err error
	defer func() {
		if err != nil {
			ierr := s.MetaStorage.AddGcobjRecord(ctx, object)
			if ierr != nil {
				log.Warnf("add gc record failed, object:%v, err:%v\n", object, ierr)
			}
		}
	}()
	if sd == nil {
		backend, err := utils.GetBackend(ctx, s.backendClient, object.Location)
		if err != nil {
			log.Errorln("failed to get backend client with err:", err)
			return err
		}
		sd, err = driver.CreateStorageDriver(backend.Type, backend)
		if err != nil {
			log.Errorln("failed to create storage, err:", err)
			return err
		}
	}

	err = sd.Delete(ctx, delInput)
	if err != nil {
		log.Warnf("clean object[%v] from backend failed, err:%v\n", err)
	}

	return err
}

func (s *s3Service) PutObjACL(ctx context.Context, in *pb.PutObjACLRequest, out *pb.BaseResponse) error {
	log.Info("PutObjACL is called in s3 service.")
	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	bucket, err := s.MetaStorage.GetBucket(ctx, in.ACLConfig.BucketName, true)
	if err != nil {
		log.Errorf("failed to get bucket meta. err: %v\n", err)
		return err
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil && isAdmin == false {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return nil
	}

	// administrator can get any resource
	if isAdmin == false {
		switch bucket.Acl.CannedAcl {
		case "bucket-owner-full-control":
			if bucket.TenantId != tenantId {
				err = ErrAccessDenied
				return err
			}
		default:
			if bucket.TenantId != tenantId {
				err = ErrAccessDenied
				return err
			}
		}
		// TODO validate user policy and ACL
	}

	object, err := s.MetaStorage.GetObject(ctx, in.ACLConfig.BucketName, in.ACLConfig.ObjectKey, "", true)
	if err != nil {
		log.Errorln("failed to get object info from meta storage. err:", err)
		return err
	}
	object.Acl = &pb.Acl{CannedAcl: in.ACLConfig.CannedAcl}
	err = s.MetaStorage.UpdateObjectMeta(object)
	if err != nil {
		log.Infoln("failed to update object meta. err:", err)
		return err
	}

	log.Infoln("Put object acl successfully.")
	return nil
}
