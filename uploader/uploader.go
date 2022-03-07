package uploader

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreservice-io/UJob"
	"github.com/coreservice-io/ULog"
	"github.com/coreservice-io/USafeGo"
	"github.com/olivere/elastic/v7"
)

const uecs_uploader = "uecs_uploader"
const letterBytes = "abcdefghijklmnopqrstuvwxyz"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func (upl *Uploader) GenRandIdStr() string {
	x := randStr(42)
	return x
}

//if Id string field is ok ,return the trimed version of it
func checkStringIdField(Iface interface{}) (string, error) {
	ValueIface := reflect.ValueOf(Iface)

	// Check if the passed interface is a pointer ,must be a pointer ,otherwise error
	if ValueIface.Type().Kind() != reflect.Ptr {
		return "", errors.New("checkStringIdField not pointer error")
	}

	// 'dereference' with Elem() and get the field by name
	Field := ValueIface.Elem().FieldByName("Id")
	if !Field.IsValid() {
		return "", errors.New("error:Interface does not have the string Id field")
	}

	var str_value string
	typestr := Field.Type().String()
	switch {
	case typestr == "string":
		str_value = Field.String()
	case typestr == "int", typestr == "int64", typestr == "int8", typestr == "int16", typestr == "int32":
		str_value = strconv.FormatInt(Field.Int(), 10)
	default:
		return "", errors.New("err:Id type error only support int/int64/int16/int8")
	}

	trimvalue := strings.TrimSpace(str_value)
	if trimvalue == "" {
		return "", errors.New("error:Id string filed is vacant")
	}
	return trimvalue, nil
}

//batch upload every 50
const UPLOAD_DEFAULT_SIZE = 100

type Uploader struct {
	client       *elastic.Client
	logs         map[string]map[string]interface{}
	logs_lock    sync.Mutex
	logs_started sync.Map
	logger       ULog.Logger
}

func (upl *Uploader) SetULogger(logger ULog.Logger) {
	upl.logger = logger
}

func (upl *Uploader) GetULogger() ULog.Logger {
	return upl.logger
}

func (upl *Uploader) AddLog_Async(indexName string, log interface{}) error {

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
		for k, v := range upl.logs[indexName] {
			if toUploadSize >= UPLOAD_DEFAULT_SIZE {
				break
			}
			toUploadSize++
			reqi := elastic.NewBulkIndexRequest().Index(indexName).Doc(v).Id(k)
			if upl.logger != nil {
				upl.logger.Traceln("uploadAnyLog_Async add record ",
					"indexName:", indexName,
					" v:", v,
					" k:", k)
			}
			bulkRequest.Add(reqi)
		}

		if toUploadSize > 0 {
			if upl.logger != nil {
				upl.logger.Debugln("uploadLog_Async  toUploadSize :", toUploadSize)
			}
			bulkresponse, _ := bulkRequest.Do(context.Background())
			if len(bulkresponse.Failed()) > 0 {
				time.Sleep(30 * time.Second)
			}
			if upl.logger != nil {
				upl.logger.Debugln("uploadLog_Async  failed count :", len(bulkresponse.Failed()))
			}

			successList := bulkresponse.Succeeded()
			upl.logs_lock.Lock()
			for i := 0; i < len(successList); i++ {
				if upl.logger != nil {
					upl.logger.Debugln("uploadLog_Async delete succeeded record :", successList[i])
				}
				delete(upl.logs[indexName], successList[i].Id)
			}
			upl.logs_lock.Unlock()
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
	resp, _ := bulkRequest.Do(context.Background())
	succeeded := resp.Succeeded()
	for i := 0; i < len(succeeded); i++ {
		succeededIds = append(succeededIds, succeeded[i].Id)
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

	USafeGo.Go(
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
				UJob.Start(uecs_uploader,
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
					2, UJob.TYPE_PANIC_REDO,
					func(job *UJob.Job) bool {
						//check to continue
						return true
					}, func(inst *UJob.Job) {
						//finally
					},
				)
			}
		}
		time.Sleep(5 * time.Second)
	}
}
