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
type SinkClient struct {
	URL   string
	Redis *redis.Pool
	ID    string
}

/*
GetClient gets a new sink connection with the given identity. Identity is used by the sink client to
introduce itself to the sink terminal.
*/
func (c *SinkConfig) GetClient(id string) (*SinkClient, error) {
	u, err := url.Parse(c.URL)
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

	pool := utils.NewRedisPool(network, address, c.Password)

	client := &SinkClient{
		ID:    id,
		URL:   strings.TrimRight(c.URL, "/"),
		Redis: pool,
	}

	return client, nil
}

func (client *SinkClient) DefaultQueue() string {
	return fmt.Sprintf("core:default:%v",
		client.ID,
	)
}

func (cl *SinkClient) GetNext(command *core.Command) error {
	db := cl.Redis.Get()
	defer db.Close()

	payload, err := redis.ByteSlices(db.Do("BLPOP", cl.DefaultQueue(), 0))
	if err != nil {
		return err
	}

	return json.Unmarshal(payload[1], command)
}

func (cl *SinkClient) Respond(result *core.JobResult) error {
	if result.ID == "" {
		return fmt.Errorf("result with no ID, not pushing results back...")
	}

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
