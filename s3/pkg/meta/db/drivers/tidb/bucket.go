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
package tidbclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/globalsign/mgo/bson"
	_ "github.com/go-sql-driver/mysql"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
)

func (t *TidbClient) GetBucket(ctx context.Context, bucketName string) (bucket *Bucket, err error) {
	log.Infof("get bucket[%s] from tidb ...\n", bucketName)
	var acl, cors, lc, policy, replication, createTime string
	var updateTime sql.NullString

	sqltext := "select bucketname,tenantid,createtime,usages,location,acl,cors,lc,policy,versioning,replication,update_time " +
		"from buckets where bucketname=?;"
	tmp := &Bucket{Bucket: &pb.Bucket{}}
	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&tmp.Name,
		&tmp.TenantId,
		&createTime,
		&tmp.Usages,
		&tmp.DefaultLocation,
		&acl,
		&cors,
		&lc,
		&policy,
		&tmp.Versioning,
		&replication,
		&updateTime,
	)
	if err != nil {
		err = handleDBError(err)
		return
	}

	ct, err := time.Parse(TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		err = handleDBError(err)
		return
	}
	tmp.CreateTime = ct.Unix()

	pbAcl := pb.Acl{}
	err = json.Unmarshal([]byte(acl), &pbAcl)
	if err != nil {
		err = handleDBError(err)
		return
	}
	tmp.Acl = &pbAcl

	err = json.Unmarshal([]byte(cors), &tmp.Cors)
	if err != nil {
		err = handleDBError(err)
		return
	}
	err = json.Unmarshal([]byte(lc), &tmp.LifecycleConfiguration)
	if err != nil {
		err = handleDBError(err)
		return
	}
	err = json.Unmarshal([]byte(policy), &tmp.BucketPolicy)
	if err != nil {
		err = handleDBError(err)
		return
	}
	err = json.Unmarshal([]byte(replication), &tmp.ReplicationConfiguration)
	if err != nil {
		err = handleDBError(err)
		return
	}

	bucket = tmp
	return
}

func (t *TidbClient) GetBuckets(ctx context.Context) (buckets []*Bucket, err error) {
	log.Info("list buckets from tidb ...")
	m := bson.M{}
	err = UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ErrInternalError
	}

	var rows *sql.Rows
	sqltext := "select bucketname,tenantid,userid,createtime,usages,location,deleted,acl,cors,lc,policy," +
		"versioning,replication,update_time from buckets;"

	tenantId, ok := m[common.CTX_KEY_TENANT_ID]
	if ok {
		sqltext = "select bucketname,tenantid,userid,createtime,usages,location,deleted,acl,cors,lc,policy," +
			"versioning,replication,update_time from buckets where tenantid=?;"
		rows, err = t.Client.Query(sqltext, tenantId)
	} else {
		rows, err = t.Client.Query(sqltext)
	}

	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		err = handleDBError(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		tmp := Bucket{Bucket: &pb.Bucket{}}
		var acl, cors, lc, policy, createTime, replication string
		var updateTime sql.NullString
		err = rows.Scan(
			&tmp.Name,
			&tmp.TenantId,
			&tmp.UserId,
			&createTime,
			&tmp.Usages,
			&tmp.DefaultLocation,
			&tmp.Deleted,
			&acl,
			&cors,
			&lc,
			&policy,
			&tmp.Versioning,
			&replication,
			&updateTime)
		if err != nil {
			err = handleDBError(err)
			return
		}
		var ctime time.Time
		ctime, err = time.ParseInLocation(TIME_LAYOUT_TIDB, createTime, time.Local)
		if err != nil {
			err = handleDBError(err)
			return
		}
		tmp.CreateTime = ctime.Unix()
		err = json.Unmarshal([]byte(acl), &tmp.Acl)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(cors), &tmp.Cors)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(lc), &tmp.LifecycleConfiguration)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(policy), &tmp.BucketPolicy)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(replication), &tmp.ReplicationConfiguration)
		if err != nil {
			err = handleDBError(err)
			return
		}
		buckets = append(buckets, &tmp)
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(ctx context.Context, bucket *Bucket) error {
	log.Infof("put bucket[%s] into tidb ...\n", bucket.Name)
	acl, _ := json.Marshal(bucket.Acl)
	cors, _ := json.Marshal(bucket.Cors)
	lc, _ := json.Marshal(bucket.LifecycleConfiguration)
	bucket_policy, _ := json.Marshal(bucket.BucketPolicy)
	sql := "update buckets set bucketname=?,acl=?,policy=?,cors=?,lc=?,tenantid=?,versioning=? where bucketname=?"
	args := []interface{}{bucket.Name, acl, bucket_policy, cors, lc, bucket.TenantId, bucket.Versioning, bucket.Name}

	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return handleDBError(err)
	}

	return nil
}

func (t *TidbClient) CheckAndPutBucket(ctx context.Context, bucket *Bucket) (bool, error) {
	var processed bool
	_, err := t.GetBucket(ctx, bucket.Name)
	if err == nil {
		processed = false
		return processed, err
	} else if err != ErrNoSuchKey {
		processed = false
		return processed, err
	} else {
		processed = true
	}
	log.Infof("insert bucket[%s] into database.\n", bucket.Name)
	sql, args := bucket.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	if err != nil {
		err = handleDBError(err)
	}
	return processed, err
}

func (t *TidbClient) ListObjects(ctx context.Context, bucketName string, versioned bool, maxKeys int,
	filter map[string]string) (retObjects []*Object, prefixes []string, truncated bool, nextMarker,
	nextVerIdMarker string, err error) {
	const MaxObjectList = 10000
	// TODO: support versioning
	if versioned {
		return
	}
	var count int
	var exit bool
	objectMap := make(map[string]struct{})
	objectNum := make(map[string]int)
	commonPrefixes := make(map[string]struct{})
	omarker := filter[common.KMarker]
	delimiter := filter[common.KDelimiter]
	prefix := filter[common.KPrefix]
	for {
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)
		sqltext = "select bucketname,name,location,tenantid,size,lastmodifiedtime,nullversion,tier from objects " +
			"where deletemarker=0 and bucketName=?"
		args = append(args, bucketName)
		var inargs []interface{}
		sqltext, inargs, err = buildSql(ctx, filter, sqltext)
		for _, v := range inargs {
			args = append(args, v)
		}
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			loopcount += 1
			obj := &Object{Object:&pb.Object{}}
			var name, lastModified string
			err = rows.Scan(
				&obj.BucketName,
				&name,
				&obj.Location,
				&obj.TenantId,
				&obj.Size,
				&lastModified,
				&obj.NullVersion,
				&obj.Tier,
			)
			if err != nil {
				err = handleDBError(err)
				return
			}
			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			if _, ok := objectNum[name]; !ok {
				objectNum[name] = 0
			}
			objectNum[name] += 1
			filter[common.KMarker] = name

			if _, ok := objectMap[name]; !ok {
				objectMap[name] = struct{}{}
			} else {
				continue
			}

			if name == omarker {
				continue
			}

			// group by delimiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					filter[common.KMarker] = prefixKey[0:(len(prefixKey) - 1)] + string(delimiter[len(delimiter) - 1] + 1)
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							truncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						nextMarker = prefixKey
						count += 1
					}
					continue
				}
			}

			count += 1
			if count == maxKeys {
				nextMarker = name
			}

			if count > maxKeys {
				truncated = true
				exit = true
				break
			}

			obj.ObjectKey = name
			lastModifiedTime, _ := time.ParseInLocation(TIME_LAYOUT_TIDB, lastModified, time.Local)
			obj.LastModified = lastModifiedTime.Unix()
			retObjects = append(retObjects, obj)
		}
		if loopcount < MaxObjectList {
			exit = true
		}
		if exit {
			break
		}
	}
	prefixes = helper.Keys(commonPrefixes)

	return
}


func (t *TidbClient) CountObjects(ctx context.Context, bucketName, prefix string) (*utils.ObjsCountInfo, error){
	var sqltext string
	rsp := utils.ObjsCountInfo{}
	var err error
	if prefix == "" {
		sqltext = "select count(*),sum(size) from objects where bucketname=?;"
		err = t.Client.QueryRow(sqltext, bucketName).Scan(&rsp.Count, &rsp.Size)
	} else {
		filt := prefix + "%"
		sqltext = "select count(*),sum(size) from objects where bucketname=? and name like ?;"
		err = t.Client.QueryRow(sqltext, bucketName, filt).Scan(&rsp.Count, &rsp.Size)
	}

	if err != nil {
		log.Errorf("db error:%v\n", err)
	}

	return &rsp, err
}

func (t *TidbClient) DeleteBucket(ctx context.Context, bucket *Bucket) error {
	sqltext := "delete from buckets where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		return handleDBError(err)
	}
	return nil
}

func (t *TidbClient) UpdateUsage(ctx context.Context, bucketName string, size int64, tx interface{}) (err error) {
	var sqlTx *sql.Tx
	if tx == nil {
		tx, err = t.Client.Begin()

		defer func() {
			if err == nil {
				err = sqlTx.Commit()
			}
			if err != nil {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)

	sql := "update buckets set usages=? where bucketname=?;"
	_, err = sqlTx.Exec(sql, size, bucketName)
	if err != nil {
		err = handleDBError(err)
	}
	return
}

func (t *TidbClient) UpdateUsages(ctx context.Context, usages map[string]int64, tx interface{}) error {
	var sqlTx *sql.Tx
	var err error
	if nil == tx {
		tx, err = t.Client.Begin()
		defer func() {
			if nil == err {
				err = sqlTx.Commit()
			} else {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)
	sqlStr := "update buckets set usages = ? where bucketname = ?;"
	st, err := sqlTx.Prepare(sqlStr)
	if err != nil {
		log.Error("failed to prepare statment with sql: ", sqlStr, ", err: ", err)
		return ErrDBError
	}
	defer st.Close()

	for bucket, usage := range usages {
		_, err = st.Exec(usage, bucket)
		if err != nil {
			log.Error("failed to update usage for bucket: ", bucket, " with usage: ", usage, ", err: ", err)
			return ErrDBError
		}
	}
	return nil
}

func buildSql(ctx context.Context, filter map[string]string, sqltxt string) (string, []interface{}, error) {
	const MaxObjectList = 10000
	args := make([]interface{}, 0)

	prefix := filter[common.KPrefix]
	if prefix != "" {
		sqltxt += " and name like ?"
		args = append(args, prefix + "%")
		log.Debug("query prefix:", prefix)
	}
	if filter[common.KMarker] != "" {
		sqltxt += " and name >= ?"
		args = append(args, filter[common.KMarker])
		log.Debug("query marker:", filter[common.KMarker])
	}

	// lifecycle management may need to filter by LastModified
	if filter[common.KLastModified] != "" {
		var tmFilter map[string]string
		err := json.Unmarshal([]byte(filter[common.KLastModified]), &tmFilter)
		if err != nil {
			log.Errorf("unmarshal lastmodified value failed:%s\n", err)
			return sqltxt, args, ErrInternalError
		}
		log.Debugf("tmpFilter:%+v\n", tmFilter)

		for k, v := range tmFilter {
			var op string
			switch k {
			case "lt":
				op = "<"
			case "gt":
				op = ">"
			case "lte":
				op = "<="
			case "gte":
				op = ">="
			default:
				log.Infof("unsupport filter action:%s\n", k)
				return sqltxt, args, ErrInternalError
			}

			sqltxt += " and lastmodifiedtime " + op + " ?"
			args = append(args, v)
		}
	}

	// lifecycle management may need to filter by StorageTier
	if filter[common.KStorageTier] != "" {
		tier, err := strconv.Atoi(filter[common.KStorageTier])
		if err != nil {
			log.Errorf("invalid storage tier:%s\n", filter[common.KStorageTier])
			return sqltxt, args, ErrInternalError
		}

		sqltxt += " and tier <= ?"
		args = append(args, tier)
	}

	delimiter := filter[common.KDelimiter]
	if delimiter == "" {
		sqltxt += " order by bucketname,name,version limit ?"
		args = append(args, MaxObjectList)
	} else {
		num := len(strings.Split(prefix, delimiter))
		if prefix == "" {
			num += 1
		}

		sqltxt += " group by SUBSTRING_INDEX(name, ?, ?) limit ?"
		args = append(args, delimiter, num, MaxObjectList)
	}

	log.Infof("sqltxt:%s\n", sqltxt)
	return sqltxt, args, nil
}

