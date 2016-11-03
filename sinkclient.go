package core

import (
	"encoding/json"
	"fmt"
	"github.com/g8os/core.base/pm/core"
	"github.com/g8os/core.base/settings"
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
type sinkClient struct {
	URL   string
	Redis *redis.Pool
	ID    string
}

/*
NewSinkClient gets a new sink connection with the given identity. Identity is used by the sink client to
introduce itself to the sink terminal.
*/
func NewSinkClient(cfg *settings.SinkConfig, id string) (SinkClient, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "redis" {
		return nil, fmt.Errorf("expected url of format redis://<host>:<port> or redis:///unix.socket")
	}

	network := "tcp"
	address := u.Host
	if address == "" {
		network = "unix"
		address = u.Path
	}

	pool := utils.NewRedisPool(network, address, cfg.Password)

	client := &sinkClient{
		ID:    id,
		URL:   strings.TrimRight(cfg.URL, "/"),
		Redis: pool,
	}

	return client, nil
}

func (client *sinkClient) String() string {
	return client.URL
}

func (client *sinkClient) DefaultQueue() string {
	return fmt.Sprintf("core:default:%v",
		client.ID,
	)
}

func (cl *sinkClient) GetNext(command *core.Command) error {
	db := cl.Redis.Get()
	defer db.Close()

	payload, err := redis.ByteSlices(db.Do("BLPOP", cl.DefaultQueue(), 0))
	if err != nil {
		return err
	}

	return json.Unmarshal(payload[1], command)
}

func (cl *sinkClient) Respond(result *core.JobResult) error {
	if result.ID == "" {
		return fmt.Errorf("result with no ID, not pushing results back...")
	}

	db := cl.Redis.Get()
	defer db.Close()

	queue := fmt.Sprintf("result:%s", result.ID)

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
