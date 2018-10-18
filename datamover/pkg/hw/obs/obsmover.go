// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

package obsmover

import (
	"bytes"
	"io"
	"obs"

	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
)

type ObsMover struct{
	obsClient *obs.ObsClient //for multipart upload
	multiUploadInitOut *obs.InitiateMultipartUploadOutput //for multipart upload
	completeParts []obs.Part //for multipart upload
}

func (mover *ObsMover)DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	obsClient, err := obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		return 0, err
	}

	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = objKey

	output, err := obsClient.GetObject(input)
	if err == nil {
		size = 0
		defer output.Body.Close()
		log.Logf("StorageClass:%s, ETag:%s, ContentType:%s, ContentLength:%d, LastModified:%s\n",
			output.StorageClass, output.ETag, output.ContentType, output.ContentLength, output.LastModified)
		var readErr error
		var readCount int = 0
		// read object
		for {
			s := buf[size:]
			readCount, readErr = output.Body.Read(s)
			//log.Logf("readCount=%d, readErr=%v\n", readCount, readErr)
			if readCount > 0 {
				size += int64(readCount)
			}
			if readErr != nil {
				log.Logf("readErr=%v\n", readErr)
				break
			}
		}
		if readErr == io.EOF {
			readErr = nil
		}
		return size, readErr
	} else if obsError, ok := err.(obs.ObsError); ok {
		log.Logf("Code:%s\n", obsError.Code)
		log.Logf("Message:%s\n", obsError.Message)
		return 0, err
	}
	return
}

func (mover *ObsMover)UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Logf("UploadObj of obsmover is called, buf.len=%d.", len(buf))
	log.Logf("buf.len:%d,buf:\n", len(buf))
	obsClient, err := obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		log.Logf("Init obs failed, err:%v.\n", err)
		return err
	}

	input := &obs.PutObjectInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	input.Body = bytes.NewReader(buf)
	output, err := obsClient.PutObject(input)
	if err != nil {
		log.Logf("Put oject to obs failed, err: %v\n", err)
	} else {
		log.Logf("Put object to obs succeed, RequestId:%s, ETag:%s\n", output.RequestId, output.ETag)
	}

	return err
}

func (mover *ObsMover)DeleteObj(objKey string, loca *LocationInfo) error {
	obsClient, err := obs.New(loca.Access, loca.Security, loca.EndPoint)
	if err != nil {
		log.Logf("New client failed when delete obj[objKey:%s] in storage backend[type:hws], err:%v\n", objKey, err)
		return err
	}

	input := &obs.DeleteObjectInput{}
	input.Bucket = loca.BucketName
	input.Key = objKey

	output, err := obsClient.DeleteObject(input)
	if err == nil {
		log.Logf("Delete object[objKey:%s] in storage backend succeed, RequestId:%s\n", objKey, output.RequestId)
	} else {
		log.Logf("Delete object[objKey:%s] in storage backend failed, err:%v\n", objKey, err)
	}

	return err
}

func (mover *ObsMover)DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = objKey
	input.RangeStart = start
	input.RangeEnd = end
	log.Logf("Try to download start:%d, end:%d\n", start, end)

	obsClient,_ := obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	output, err := obsClient.GetObject(input)
	if err != nil {
		log.Logf("GetObject failed, err:%v\n", err)
		return 0,err
	}
	defer output.Body.Close()

	readCount := 0
	var readErr error
	for {
		var rc int
		rc, readErr = output.Body.Read(buf)
		if rc > 0 {
			readCount += rc
		}
		if readErr != nil {
			break
		}
	}
	if readErr != nil && readErr != io.EOF{
		//log.Logf("%s", buf[:readCount])
		log.Logf("Body.read failed, err:%v\n", err)
		return 0,readErr
	}

	//log.Logf("Download readCount=%d\nbuf:%s\n", readCount,buf[:readCount])
	log.Logf("Download readCount=%d\n", readCount)
	return int64(readCount), nil
}

func (mover *ObsMover)MultiPartUploadInit(objKey string, destLoca *LocationInfo) error{
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	var err error = nil
	mover.obsClient,err = obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		log.Logf("Create obsclient failed, err:%v\n", err)
		return err
	}
	mover.multiUploadInitOut,err = mover.obsClient.InitiateMultipartUpload(input)
	if err != nil {
		log.Logf("InitiateMultipartUpload failed, err:%v\n", err)
		return err
	}

	return nil
}

func (mover *ObsMover)UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumer int64, offset int64) error {
	uploadPartInput := &obs.UploadPartInput{}
	uploadPartInput.Bucket = destLoca.BucketName
	uploadPartInput.Key = objKey
	uploadPartInput.UploadId = mover.multiUploadInitOut.UploadId
	uploadPartInput.Body = bytes.NewReader(buf)
	uploadPartInput.PartNumber = int(partNumer)
	uploadPartInput.Offset = offset
	uploadPartInput.PartSize = upBytes
	tries := 1
	for tries <= 3 {
		uploadPartInputOutput, err := mover.obsClient.UploadPart(uploadPartInput)
		if err != nil {
			if tries == 3 {
				log.Logf("Upload part to hws failed. err:%v\n", err)
				return err
			}
			log.Logf("Retrying to upload part#%d\n", partNumer)
			tries++
		}else {
			log.Logf("Upload part %d finished, offset:%d, size:%d\n", partNumer, offset, upBytes)
			mover.completeParts = append(mover.completeParts, obs.Part{
				ETag: uploadPartInputOutput.ETag,
				PartNumber: uploadPartInputOutput.PartNumber})
			break
		}
	}

	return nil
}

func (mover *ObsMover)AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	input := &obs.AbortMultipartUploadInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	input.UploadId = mover.multiUploadInitOut.UploadId

	_, err := mover.obsClient.AbortMultipartUpload(input)
	log.Logf("Abort multipartupload finish, uploadId:%s, err:%v\n", mover.multiUploadInitOut.UploadId, err)

	return err
}

func (mover *ObsMover)CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	completeMultipartUploadInput := &obs.CompleteMultipartUploadInput{}
	completeMultipartUploadInput.Bucket = destLoca.BucketName
	completeMultipartUploadInput.Key = objKey
	completeMultipartUploadInput.UploadId = mover.multiUploadInitOut.UploadId
	completeMultipartUploadInput.Parts = mover.completeParts
	_, err := mover.obsClient.CompleteMultipartUpload(completeMultipartUploadInput)
	if err != nil {
		//panic(err)
		log.Logf("CompleteMultipartUpload failed, err:%v\n", err)
	}

	return err
}