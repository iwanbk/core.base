package settings

import (
	"encoding/json"
	"fmt"
	"github.com/g8os/core.base/pm/core"
	"github.com/g8os/core.base/utils"
	"github.com/garyburd/redigo/redis"
	"net/url"
	"strings"
)

const (
	ReturnExpire = 300
)

/*
ControllerClient represents an active agent controller connection.
*/
type ControllerClient struct {
	URL   string
	Redis *redis.Pool
}

/*
NewControllerClient gets a new agent controller connection
*/
func (c *Controller) GetClient() (*ControllerClient, error) {
	u, err := url.Parse(c.URL)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "redis" {
		return nil, fmt.Errorf("expected url of format redis://<host>:<port>")
	}

	pool := utils.NewRedisPool(u.Host, c.Password)
	if _, err := pool.Get().Do("PING"); err != nil {
		return nil, err
	}

	client := &ControllerClient{
		URL:   strings.TrimRight(c.URL, "/"),
		Redis: pool,
	}

	return client, nil
}

func (client *ControllerClient) DefaultQueue() string {
	return fmt.Sprintf("core:default:%v:%v",
		Options.Gid(),
		Options.Nid(),
	)
}

func (cl *ControllerClient) GetNext(command *core.Command) error {
	db := cl.Redis.Get()
	defer db.Close()

	payload, err := redis.Bytes(db.Do("BLPOP", cl.DefaultQueue()))
	if err != nil {
		return err
	}

	return json.Unmarshal(payload, command)
}

func (cl *ControllerClient) Respond(result *core.JobResult) error {
	db := cl.Redis.Get()
	defer db.Close()

	queue := fmt.Sprintf("cmd:%s", result.ID)

	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}

	if _, err := db.Do("RPUSH", queue, payload); err != nil {
		return err
	}
	if _, err := db.Do("EXPIRE", queue, ReturnExpire); err != nil {
		return err
	}

	return nil
}
