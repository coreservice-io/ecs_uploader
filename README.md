# ecs_uploader
elastic/open search 

#### Important!
```
for each struct to upload must have a "Id" filed of type 
string/int/uint/int8/uint8/int16/uint16/int32/uint32/int64/uint64
otherwise it will fail
```

#### example
```go
package main

import (
	"fmt"
	"time"

	"github.com/coreservice-io/ecs_uploader/uploader"
	"github.com/coreservice-io/log"
	"github.com/coreservice-io/logrus_log"
)

type example struct {
	Id  int
	hex string
}

func main() {

	////
	llog, err := logrus_log.New("./logs", 1, 20, 30)
	if err != nil {
		panic(err.Error())
	}
	llog.SetLevel(log.InfoLevel)

	ecs_endpoint := "xxxx"
	ecs_account := "yyyy"
	ecs_pass := "zzzz"

	ecs_uploader, err := uploader.New(ecs_endpoint, ecs_account, ecs_pass)
	if err != nil {
		return
	} else {
		ecs_uploader.SetLogger(llog)
		sids, err := ecs_uploader.AddLogs_Sync("example", []interface{}{
			&example{Id: 1, hex: "hex"},
			&example{Id: 10, hex: "hexhex"},
		})

		if err != nil {
			panic(err)
		} else {
			fmt.Println(sids)
		}

		for i := 0; i < 100; i++ {
			//ecs_uploader.AddLog_Async("example", &example{Id: i, hex:"hex"})
		}
		time.Sleep(time.Second * 100)
	}
}


```
