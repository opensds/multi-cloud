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

package plan

import (
	"regexp"
	"time"

	"encoding/json"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	c "github.com/opensds/multi-cloud/api/pkg/filters/context"
	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/dataflow/pkg/scheduler/trigger"
	"github.com/opensds/multi-cloud/datamover/proto"
	"github.com/opensds/multi-cloud/dataflow/pkg/kafka"
	//"golang.org/x/net/context"
)

var dataBaseName = "test"
var tblConnector = "connector"
var tblPolicy = "policy"
var topicMigration = "migration"


func isEqual(src *Connector, dest *Connector) bool {
	switch src.StorType {
	case STOR_TYPE_OPENSDS:
		if dest.StorType == STOR_TYPE_OPENSDS && src.BucketName == dest.BucketName {
			return true
		} else {
			return false
		}
	default: //TODO: check according to StorType later.
		return false
	}
}

func Create(ctx *c.Context, plan *Plan) (*Plan, error) {
	//Check parameter validity
	m, err := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
	if !m || plan.Name == "all" {
		log.Logf("Invalid plan name[%s], err:%v", plan.Name, err)
		return nil, ERR_INVALID_PLAN_NAME
	}

	plan.Id = ""
	plan.LastSchedTime = 0 //set to be 0 as default

	if isEqual(&plan.SourceConn, &plan.DestConn) {
		log.Log("source connector is the same as destination connector.")
		return nil, ERR_DEST_SRC_CONN_EQUAL
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{tblPolicy, bson.ObjectIdHex(plan.PolicyId), dataBaseName}
		} else {
			log.Logf("Invalid policy:%s\n", plan.PolicyId)
			return nil, ERR_POLICY_NOT_EXIST
		}
	}

	//Add to database
	plan, err = db.DbAdapter.CreatePlan(ctx, plan)
	if err != nil {
		log.Logf("create plan(%s) in db failed,%v", err)
		return nil, err
	}

	if plan.PolicyId != "" && plan.PolicyEnabled {
		if err := trigger.GetTriggerMgr().Add(ctx, plan, NewPlanExecutor(ctx, plan)); err != nil {
			log.Logf("Add plan(%s) to trigger failed, %v", plan.Id.Hex(), err)
			return nil, err
		}
	}

	return plan, nil
}

func Delete(ctx *c.Context, id string) error {

	plan, err := db.DbAdapter.GetPlan(ctx, id)
	if err == ERR_PLAN_NOT_EXIST {
		log.Logf("specified plan(%s) is not exist, ignore it ", id)
		return nil
	}

	if err != nil {
		log.Logf("Delete plan failed, %v", err)
		return err
	}

	if plan.PolicyId != "" {
		err = trigger.GetTriggerMgr().Remove(ctx, plan)
		if err != nil && err != ERR_PLAN_NOT_EXIST {
			log.Logf("Remove plan from triggers failed, %v", err)
			return err
		}
	}

	return db.DbAdapter.DeletePlan(ctx, id)
}

//1. cannot update type
func Update(ctx *c.Context, planId string, updateMap map[string]interface{}) (*Plan, error) {

	curPlan, err := db.DbAdapter.GetPlan(ctx, planId)
	if err != nil {
		log.Logf("Update plan failed, err: can not get the plan(%v).\n", err.Error())
		return nil, err
	}

	if v, ok := updateMap["name"]; ok {
		name := v.(string)
		m, err := regexp.MatchString("[[:alnum:]-_.]+", name)
		if !m {
			log.Logf("Invalid plan name[%s],err:", name, err) //cannot use all as name
			return nil, ERR_INVALID_PLAN_NAME
		}
		curPlan.Name = name
	}

	if v, ok := updateMap["sourceConn"]; ok {
		b, _ := json.Marshal(v)
		curPlan.SourceConn = Connector{}
		json.Unmarshal(b, &curPlan.SourceConn)
	}

	if v, ok := updateMap["destConn"]; ok {
		b, _ := json.Marshal(v)
		curPlan.DestConn = Connector{}
		json.Unmarshal(b, &curPlan.DestConn)
	}

	if v, ok := updateMap["filter"]; ok {
		b, _ := json.Marshal(v)
		curPlan.Filter = Filter{Prefix: "/"}
		json.Unmarshal(b, &curPlan.Filter)
	}

	if isEqual(&curPlan.SourceConn, &curPlan.DestConn) {
		log.Log("source connector is the same as destination connector.")
		return nil, ERR_DEST_SRC_CONN_EQUAL
	}

	var needUpdateTrigger = false
	if v, ok := updateMap["policyEnabled"]; ok {
		curPlan.PolicyEnabled = v.(bool)
		needUpdateTrigger = true
	}

	if v, ok := updateMap["policyId"]; ok {
		curPlan.PolicyId = v.(string)
		needUpdateTrigger = true
		if curPlan.PolicyId == "" {
			curPlan.PolicyRef = mgo.DBRef{}
		} else if bson.IsObjectIdHex(curPlan.PolicyId) {
			curPlan.PolicyRef = mgo.DBRef{tblPolicy, bson.ObjectIdHex(curPlan.PolicyId), dataBaseName}
		} else {
			log.Logf("Invalid policy:%s\n", curPlan.PolicyId)
			return nil, ERR_POLICY_NOT_EXIST
		}

	}

	if needUpdateTrigger {
		trigger.GetTriggerMgr().Remove(ctx, curPlan)
		if curPlan.PolicyId != "" && curPlan.PolicyEnabled {
			if err := trigger.GetTriggerMgr().Add(ctx, curPlan, NewPlanExecutor(ctx, curPlan)); err != nil {
				log.Logf("Add plan(%s) to trigger failed, %v", curPlan.Id.Hex(), err)
				return nil, err
			}
		}
	}

	return db.DbAdapter.UpdatePlan(ctx, curPlan)
}

func Get(ctx *c.Context, id string) (*Plan, error) {
	return db.DbAdapter.GetPlan(ctx, id)
}

func List(ctx *c.Context) ([]Plan, error) {
	return db.DbAdapter.ListPlan(ctx)
}

func getLocation(conn *Connector) (string, error) {
	switch conn.StorType {
	case STOR_TYPE_OPENSDS:
		return conn.BucketName, nil
	default:
		log.Logf("Unsupport cnnector type:%v, return ERR_INNER_ERR\n", conn.StorType)
		return "", ERR_INNER_ERR
	}
}

func sendJob(req *datamover.RunJobRequest) error{
	data, err := json.Marshal(*req)
	if err != nil {
		log.Logf("Marshal run job request failed, err:%v\n", data)
		return err
	}

	return kafka.ProduceMsg(topicMigration, data)
}

/*func sendJob(req *datamover.RunJobRequest, mclient datamover.DatamoverService) error{
	ch := make(chan int)
	go func(req *datamover.RunJobRequest) {
		//TODO: call mclient.Runjob directly is a temporary way, need to use sending message to kafka replace it.
		ctx := context.Background()
		_, ok := ctx.Deadline()
		if !ok {
			ctx, _ = context.WithTimeout(ctx, 7200*time.Second)
		}
		_, err := mclient.Runjob(ctx, req)
		if err != nil {
			log.Logf("Run job failed, err:%v\n", err)
			ch <- 1
		} else {
			log.Log("Run job succeed.")
			ch <- 0
		}
	}(req)

	select {
	case n := <-ch:
		log.Logf("Run job end, n=%d\n", n)
	case <-time.After(86400 * time.Second):
		log.Log("Wait job timeout.")
	}

	return nil
}*/

func buildConn(reqConn *datamover.Connector, conn *Connector) {
	if conn.StorType == STOR_TYPE_OPENSDS {
		reqConn.BucketName = conn.BucketName
	} else {
		for i := 0; i < len(conn.ConnConfig); i++ {
			reqConn.ConnConfig = append(reqConn.ConnConfig, &datamover.KV{Key: conn.ConnConfig[i].Key, Value: conn.ConnConfig[i].Value})
		}
	}
}

func Run(ctx *c.Context, id string) (bson.ObjectId, error) {
	//Get information from database
	plan, err := db.DbAdapter.GetPlan(ctx, id)
	if err != nil {
		return "", err
	}

	//scheduling must be mutual excluded among several schedulers
	//Get Lock
	ret := db.DbAdapter.LockSched(string(plan.Id.Hex()))
	for i := 0; i < 3; i++ {
		if ret == LockSuccess {
			//Make sure unlock before return
			defer db.DbAdapter.UnlockSched(string(plan.Id.Hex()))
			break
		} else if ret == LockBusy {
			return "", ERR_RUN_PLAN_BUSY
		} else {
			//Try to lock again, try three times at most
			ret = db.DbAdapter.LockSched(string(plan.Id.Hex()))
		}
	}

	//Get source location by source connector
	srcLocation, err1 := getLocation(&plan.SourceConn)
	if err1 != nil {
		return "", err1
	}

	//Get destination location by destination connector
	destLocation, err2 := getLocation(&plan.DestConn)
	if err2 != nil {
		return "", err2
	}

	ct := time.Now()
	//Create job
	job := Job{}
	//obId := bson.NewObjectId()
	//job.Id = jobId
	job.Type = plan.Type
	job.PlanId = string(plan.Id.Hex())
	job.PlanName = plan.Name
	job.SourceLocation = srcLocation
	job.DestLocation = destLocation
	job.CreateTime = ct
	job.Status = JOB_STATUS_PENDING
	job.OverWrite = plan.OverWrite
	job.RemainSource = plan.RemainSource

	//add job to database
	_, err = db.DbAdapter.CreateJob(ctx, &job)
	if err == nil {
		//TODO: change to send job to datamover by kafka
		//This way send job is the temporary
		req := datamover.RunJobRequest{Id:job.Id.Hex(), OverWrite:plan.OverWrite, RemainSource:plan.RemainSource}
		srcConn := datamover.Connector{Type:plan.SourceConn.StorType}
		buildConn(&srcConn, &plan.SourceConn)
		req.SourceConn = &srcConn
		destConn := datamover.Connector{Type: plan.DestConn.StorType}
		buildConn(&destConn, &plan.DestConn)
		req.DestConn = &destConn
		go sendJob(&req)
	} else {
		log.Logf("Add job[id=%s,plan=%s,source_location=%s,dest_location=%s] to database failed.\n", string(job.Id.Hex()),
			job.PlanName, job.SourceLocation, job.DestLocation)
	}

	return job.Id, nil
}

type TriggerExecutor struct {
	planId    string
	tenantId  string
	ctx       *c.Context
}

func NewPlanExecutor(ctx *c.Context, plan *Plan) trigger.Executer {
	return &TriggerExecutor{
		planId:    plan.Id.Hex(),
		tenantId:  plan.Tenant,
		ctx:       ctx,
	}
}

func (p *TriggerExecutor) Run() {
	log.Logf("Plan (%s) is called in dataflow service.", p.planId)
	//tenant := "tenant"
	//jobId, err := Run(p.planId, tenant, p.datamover)
	//if err != nil {
	//	log.Logf("PlanExcutor run plan(%s) error, jobid:%s, error:%v",p.planId, jobId, err)
	//}
}
