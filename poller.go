package core

import (
	"github.com/g8os/core.base/pm"
	"github.com/g8os/core.base/pm/core"
	"github.com/g8os/core.base/settings"
	"time"
)

const (
	ReconnectSleepTime = 10 * time.Second
)

type Sink interface {
	Run()
}

type redisSink struct {
	key        string
	mgr        *pm.PM
	controller *settings.ControllerClient
}

func getKeys(m map[string]*settings.ControllerClient) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	return keys
}

func NewSink(key string, mgr *pm.PM, controller *settings.ControllerClient) Sink {
	poll := &redisSink{
		key:        key,
		mgr:        mgr,
		controller: controller,
	}

	return poll
}

func (poll *redisSink) handler(cmd *core.Command, result *core.JobResult) {
	if err := poll.controller.Respond(result); err != nil {
		log.Errorf("Failed to respond to command %s: %s", cmd, err)
	}
}

func (poll *redisSink) run() {
	lastError := time.Now()

	poll.mgr.AddRouteResultHandler(core.Route(poll.key), poll.handler)

	for {
		var command core.Command
		err := poll.controller.GetNext(&command)
		if err != nil {
			log.Errorf("Failed to get next command from %s: %s", poll.controller.URL, err)
			if time.Now().Sub(lastError) < ReconnectSleepTime {
				time.Sleep(ReconnectSleepTime)
			}
			lastError = time.Now()

			continue
		}

		command.Route = core.Route(poll.key)

		command.Gid = settings.Options.Gid()
		command.Nid = settings.Options.Nid()

		log.Infof("Starting command %s", command)

		if command.Queue == "" {
			poll.mgr.PushCmd(&command)
		} else {
			poll.mgr.PushCmdToQueue(&command)
		}
	}
}

func (poll *redisSink) Run() {
	go poll.run()
}

/*
StartSinks starts the long polling routines and feed the manager with received commands
*/
func StartSinks(mgr *pm.PM, controllers map[string]*settings.ControllerClient) {
	var keys []string
	if len(settings.Settings.Channel.Cmds) > 0 {
		keys = settings.Settings.Channel.Cmds
	} else {
		keys = getKeys(controllers)
	}

	for _, key := range keys {
		controller, ok := controllers[key]
		if !ok {
			log.Fatalf("No contoller with name '%s'", key)
		}

		poll := NewSink(key, mgr, controller)
		poll.Run()
	}
}