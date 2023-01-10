package uploader

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreservice-io/job"
	"github.com/coreservice-io/log"
	"github.com/coreservice-io/safe_go"
	"github.com/olivere/elastic/v7"
)

const uecs_uploader = "uecs_uploader"

// if Id string field is ok ,return the trimed version of it
func checkStringIdField(Iface interface{}) (string, error) {
	ValueIface := reflect.ValueOf(Iface)

	var Field reflect.Value
	if ValueIface.Type().Kind() == reflect.Ptr {
		Field = ValueIface.Elem().FieldByName("Id")
		if !Field.IsValid() {
			return "", errors.New("error:Interface does not have the string Id field")
		}
	} else {
		Field = ValueIface.FieldByName("Id")
		if !Field.IsValid() {
			return "", errors.New("error:Interface does not have the string Id field")
		}
	}

	var str_value string
	typestr := Field.Type().String()
	switch {
	case typestr == "string":
		str_value = Field.String()
	case typestr == "int", typestr == "uint", typestr == "int64", typestr == "uint64",
		typestr == "int8", typestr == "uint8", typestr == "int16", typestr == "uint16", typestr == "int32", typestr == "uint32":
		str_value = strconv.FormatInt(Field.Int(), 10)
	default:
		return "", errors.New("err:Id type error only support string/int/uint/int8/uint8/int16/uint16/int32/uint32/int64/uint64")
	}

	trimvalue := strings.TrimSpace(str_value)
	if trimvalue == "" {
		return "", errors.New("error:Id string filed is vacant")
	}

	return trimvalue, nil
}

// batch upload every 50
const UPLOAD_DEFAULT_SIZE = 100

type Uploader struct {
	client       *elastic.Client
	logs         map[string]map[string]interface{}
	logs_lock    sync.Mutex
	logs_started sync.Map
	logger       log.Logger
}

func (upl *Uploader) SetLogger(logger log.Logger) {
	upl.logger = logger
}

func (upl *Uploader) GetLogger() log.Logger {
	return upl.logger
}

// logs may get lost if upload failed
func (upl *Uploader) AddLog_Async_Unsafe(indexName string, log interface{}) error {
	idstr, iderr := checkStringIdField(log)
	if iderr != nil {
		if upl.logger != nil {
			upl.logger.Errorln(iderr)
		}
		return iderr
	}
	upl.logs_lock.Lock()
	_, ok := upl.logs[indexName]
	if !ok {
		upl.logs[indexName] = make(map[string]interface{})
	}
	upl.logs[indexName][idstr] = log
	upl.logs_lock.Unlock()
	return nil
}

func (upl *Uploader) uploadLog_Async(indexName string) {
	bulkRequest := upl.client.Bulk()
	for {
		toUploadSize := 0
		toUploadDocs := make(map[string]interface{})

		upl.logs_lock.Lock()
		for k, v := range upl.logs[indexName] {
			if toUploadSize >= UPLOAD_DEFAULT_SIZE {
				break
			}
			toUploadSize++
			toUploadDocs[k] = v
		}

		for k, v := range toUploadDocs {
			if upl.logger != nil {
				upl.logger.Debugln("uploadLog_Async delete local record to upload:", k)
			}
			delete(upl.logs[indexName], k)
			reqi := elastic.NewBulkIndexRequest().Index(indexName).Doc(v).Id(k)
			if upl.logger != nil {
				upl.logger.Traceln("uploadAnyLog_Async add record ",
					"indexName:", indexName,
					" v:", v,
					" k:", k)
			}
			bulkRequest.Add(reqi)
		}

		upl.logs_lock.Unlock()
		if toUploadSize > 0 {
			if upl.logger != nil {
				upl.logger.Debugln("uploadLog_Async  toUploadSize :", toUploadSize)
			}
			bulkresponse, req_err := bulkRequest.Do(context.Background())
			if req_err != nil {
				time.Sleep(15 * time.Second)
				upl.logger.Errorln("uploadLog_Async request error :", req_err.Error())
			}
			if len(bulkresponse.Failed()) > 0 {
				time.Sleep(15 * time.Second)
				if upl.logger != nil {
					upl.logger.Errorln("uploadLog_Async failed count :", len(bulkresponse.Failed()))
				}
			}
			if len(bulkresponse.Succeeded()) > 0 {
				if upl.logger != nil {
					upl.logger.Debugln("uploadLog_Async succeeded count :", len(bulkresponse.Succeeded()))
				}
			}
		}

		//give warnings to system
		if len(upl.logs[indexName]) > 1000000 {
			if upl.logger != nil {
				upl.logger.Errorln(
					"uploader",
					"LogOverFlow:"+indexName,
					"!!error critical!! UserDefinedMapLogs:"+indexName+": length too big > 1000000",
					time.Now().Unix(),
				)
			}
		}
		//wait and add
		if len(upl.logs[indexName]) == 0 {
			time.Sleep(5 * time.Second)
		}
	}

}

func (upl *Uploader) AddLogs_Sync(indexName string, logs []interface{}) (succeededIds []string, idInputError error) {
	if len(logs) == 0 {
		return []string{}, nil
	}
	bulkRequest := upl.client.Bulk()
	for i := 0; i < len(logs); i++ {
		idstr, iderr := checkStringIdField(logs[i])
		if iderr != nil {
			return []string{}, iderr
		}
		reqi := elastic.NewBulkIndexRequest().Index(indexName).Doc(logs[i]).Id(idstr)
		bulkRequest.Add(reqi)
	}
	resp, bulk_req_err := bulkRequest.Do(context.Background())
	if bulk_req_err != nil {
		return []string{}, errors.New("addLogs_Sync request err:" + bulk_req_err.Error())
	}

	succeeded := resp.Succeeded()
	for i := 0; i < len(succeeded); i++ {
		succeededIds = append(succeededIds, succeeded[i].Id)
	}
	if len(succeededIds) == 0 {
		return succeededIds, errors.New("nothing uploaded error")
	}
	return succeededIds, nil
}

//////end of UserDefinedLogs/////////////

func New(endpoint string, username string, password string) (*Uploader, error) {

	client, err := elastic.NewClient(
		elastic.SetURL(endpoint),
		elastic.SetBasicAuth(username, password),
		elastic.SetSniff(false),
		elastic.SetHealthcheckInterval(30*time.Second),
		elastic.SetGzip(true),
	)
	if err != nil {
		return nil, err
	}

	upl := &Uploader{
		client: client,
		logs:   make(map[string]map[string]interface{}),
	}

	safe_go.Go(
		// process
		func(args ...interface{}) {
			upl.start()
		},
		// onPanic callback
		func(err interface{}) {
			if upl.logger != nil {
				upl.logger.Errorln(uecs_uploader, err)
			}
		})
	return upl, nil
}

func (upl *Uploader) start() {
	for {
		for lmkindex, _ := range upl.logs {
			_, ok := upl.logs_started.Load(lmkindex)
			if !ok {
				upl.logs_started.Store(lmkindex, true)
				job.Start(uecs_uploader,
					func() {
						// job process
						upl.uploadLog_Async(lmkindex)
					}, func(err interface{}) {
						// onPanic callback, run if panic happened
						if upl != nil {
							upl.logger.Errorln(uecs_uploader, err)
						}
						time.Sleep(30 * time.Second)
					},
					2, job.TYPE_PANIC_REDO,
					func(job *job.Job) bool {
						//check to continue
						return true
					}, func(inst *job.Job) {
						//finally
					},
				)
			}
		}
		time.Sleep(5 * time.Second)
	}
}
