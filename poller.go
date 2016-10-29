package core

import (
	"github.com/g8os/core.base/pm"
	"github.com/g8os/core.base/pm/core"
	"github.com/g8os/core.base/settings"
	"net/url"
	"time"
)

const (
	ReconnectSleepTime = 10 * time.Second
)

type poller struct {
	key        string
	controller *settings.ControllerClient
}

func getKeys(m map[string]*settings.ControllerClient) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	return keys
}

func newPoller(key string, controller *settings.ControllerClient) *poller {
	poll := &poller{
		key:        key,
		controller: controller,
	}

	return poll
}

func (poll *poller) longPoll() {
	lastError := time.Now()

	pollQuery := make(url.Values)

	for _, role := range settings.Options.Roles() {
		pollQuery.Add("role", role)
	}

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

		//tag command for routing.
		//ctrlConfig := controller.Config
		//cmd.Args.SetTag(poll.key)
		//cmd.Args.SetController(ctrlConfig)

		command.Gid = settings.Options.Gid()
		command.Nid = settings.Options.Nid()

		log.Infof("Starting command %s", command)

		if command.Queue == "" {
			pm.GetManager().PushCmd(&command)
		} else {
			pm.GetManager().PushCmdToQueue(&command)
		}
	}
}

/*
StartPollers starts the long polling routines and feed the manager with received commands
*/
func StartPollers(controllers map[string]*settings.ControllerClient) {
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

		poll := newPoller(key, controller)
		go poll.longPoll()
	}
}
