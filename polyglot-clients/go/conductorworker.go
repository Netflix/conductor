// Copyright 2017 Netflix, Inc.
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
package conductor

import (
	"github.com/netflix/conductor/client/go/task"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

var (
	hostname, hostnameError = os.Hostname()
)

func init() {
	if hostnameError != nil {
		log.Fatal("Could not get hostname")
	}
}

type ConductorWorker struct {
	ConductorHttpClient *ConductorHttpClient
	ThreadCount         int
	PollingInterval     int
}

// NewConductorWorker Create a new Conductor worker
//with baseUrl (e.g. http://localhost:8080/api, and pollingInterval in millisecond)
func NewConductorWorker(baseUrl string, threadCount int, pollingInterval int) *ConductorWorker {
	conductorWorker := new(ConductorWorker)
	conductorWorker.ThreadCount = threadCount
	conductorWorker.PollingInterval = pollingInterval
	conductorHttpClient := NewConductorHttpClient(baseUrl)
	conductorWorker.ConductorHttpClient = conductorHttpClient
	return conductorWorker
}

// NewConductorWorkerWithConfig Create a new Conductor worker with configuration
func NewConductorWorkerWithConfig(cfg ConductorHttpClientConfig, threadCount int, pollingInterval int) *ConductorWorker {
	conductorWorker := new(ConductorWorker)
	conductorWorker.ThreadCount = threadCount
	conductorWorker.PollingInterval = pollingInterval
	conductorHttpClient := NewConductorHttpClientWithConfig(cfg)
	conductorWorker.ConductorHttpClient = conductorHttpClient
	return conductorWorker
}

func (c *ConductorWorker) Execute(t *task.Task, executeFunction func(t *task.Task) (*task.TaskResult, error)) {
	taskResult, err := executeFunction(t)
	if taskResult == nil {
		log.Error("TaskResult cannot be nil: ", t.TaskId)
		return
	}
	if err != nil {
		log.Error("Error Executing task:", err.Error())
		taskResult.Status = task.FAILED
		taskResult.ReasonForIncompletion = err.Error()
	}

	taskResultJsonString, err := taskResult.ToJSONString()
	if err != nil {
		log.Error("Error Forming TaskResult JSON body", err)
		return
	}
	_, _ = c.ConductorHttpClient.UpdateTask(taskResultJsonString)
}

func (c *ConductorWorker) PollAndExecute(taskType string, domain string, executeFunction func(t *task.Task) (*task.TaskResult, error)) {
	for {
		time.Sleep(time.Duration(c.PollingInterval) * time.Millisecond)

		// Poll for Task taskType
		polled, err := c.ConductorHttpClient.PollForTask(taskType, hostname, domain)
		if err != nil {
			log.Error("Error Polling task:", err.Error())
			continue
		}
		if polled == "" {
			log.Debug("No task found for:", taskType)
			continue
		}

		// Parse Http response into Task
		parsedTask, err := task.ParseTask(polled)
		if err != nil {
			log.Error("Error Parsing task:", err.Error())
			continue
		}

		// Execute given function
		c.Execute(parsedTask, executeFunction)
	}
}

func (c *ConductorWorker) Start(taskType string, domain string, executeFunction func(t *task.Task) (*task.TaskResult, error), wait bool) {
	log.Println("Polling for task:", taskType, "with a:", c.PollingInterval, "(ms) polling interval with", c.ThreadCount, "goroutines for task execution, with workerid as", hostname)
	for i := 1; i <= c.ThreadCount; i++ {
		go c.PollAndExecute(taskType, domain, executeFunction)
	}

	// wait infinitely while the go routines are running
	if wait {
		select {}
	}
}
