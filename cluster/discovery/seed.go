package discovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	rdb "github.com/redis/go-redis/v9"
	"github.com/wind-c/comqtt/v2/cluster/log"
	"github.com/wind-c/comqtt/v2/cluster/storage/redis"
	"github.com/wind-c/comqtt/v2/cluster/utils"
	"github.com/wind-c/comqtt/v2/config"
)

const (
	logTag                      string = "seedRegistry"
	defaultNodeNamePrefix       string = "co-"
	defaultEventLoopIntervalSec int64  = 10
	defaultNodesRegistryKey     string = "nodes"
	defaultNodeRegistryExp      int64  = 30
	defaultLockKey              string = "node-registry-mutex"
	defaultLockLoopIntervalSec  uint   = 5
	defaultMaxLockAttempts      int    = 30
)

const (
	AddressWayBindAddr uint = iota
	AddressWayPrivateIP
	AddressWayPublicIP
	AddressWayHostname
)

const (
	NodeNameWayNodeName uint = iota
	NodeNameWayPrivateIP
	NodeNameWayPublicIP
	NodeNameWayHostname
	NodeNameWayUUID
)

var (
	ErrKeyExpMustBeGreater = errors.New("redis-node-key-exp must be greater than event-loop-interval-sec")
	ErrRedisNotAvail       = errors.New("redis not available")
	ErrInvalidAddress      = errors.New("invalid or missing address")
	ErrInvalidNodeName     = errors.New("invalid or missing node name")
)

type SeedRegistry struct {
	NodeKey       string
	NodesFilePath string
	cfg           *config.Cluster
	ctx           context.Context
	term          chan bool
	rdsync        *redsync.Redsync
	lock          *redsync.Mutex
}

func NewSeedRegistry(cfg *config.Cluster) *SeedRegistry {
	pool := goredis.NewPool(redis.Client())
	return &SeedRegistry{
		term:   make(chan bool, 1),
		cfg:    cfg,
		ctx:    context.Background(),
		rdsync: redsync.New(pool),
	}
}

func (r *SeedRegistry) Start() (err error) {

	// seed registry disabled
	if !r.cfg.SeedRegistry.Enable {
		return
	}

	// setup defaults

	if len(r.cfg.SeedRegistry.NodeNamePrefix) == 0 {
		r.cfg.SeedRegistry.NodeNamePrefix = defaultNodeNamePrefix
	}
	if len(r.cfg.SeedRegistry.NodesRegistryKey) == 0 {
		r.cfg.SeedRegistry.NodesRegistryKey = defaultNodesRegistryKey
	}
	if len(r.cfg.SeedRegistry.LockKey) == 0 {
		r.cfg.SeedRegistry.LockKey = defaultLockKey
	}

	// these should never be zero
	if r.cfg.SeedRegistry.EventLoopIntervalSec == 0 {
		r.cfg.SeedRegistry.EventLoopIntervalSec = defaultEventLoopIntervalSec
	}
	if r.cfg.SeedRegistry.NodeRegistryExp == 0 {
		r.cfg.SeedRegistry.NodeRegistryExp = defaultNodeRegistryExp
	}
	if r.cfg.SeedRegistry.LockLoopIntervalSec == 0 {
		r.cfg.SeedRegistry.LockLoopIntervalSec = defaultLockLoopIntervalSec
	}
	if r.cfg.SeedRegistry.MaxLockAttempts == 0 {
		r.cfg.SeedRegistry.MaxLockAttempts = defaultMaxLockAttempts
	}

	log.Info(fmt.Sprintf("using node name prefix: %s", r.cfg.SeedRegistry.NodeNamePrefix), logTag, "startup")
	log.Info(fmt.Sprintf("using nodes registry table key: %s", r.cfg.SeedRegistry.NodesRegistryKey), logTag, "startup")
	log.Info(fmt.Sprintf("using node exp: %d secs", r.cfg.SeedRegistry.NodeRegistryExp), logTag, "startup")
	log.Info(fmt.Sprintf("using event loop interval: %d secs", r.cfg.SeedRegistry.EventLoopIntervalSec), logTag, "startup")
	log.Info(fmt.Sprintf("using lock key: %s", r.cfg.SeedRegistry.LockKey), logTag, "startup")
	log.Info(fmt.Sprintf("using lock frequency: %d secs", r.cfg.SeedRegistry.LockLoopIntervalSec), logTag, "startup")
	log.Info(fmt.Sprintf("using max lock attempts: %d", r.cfg.SeedRegistry.MaxLockAttempts), logTag, "startup")

	// cluster mode also requires redis, so we shouldn't need to validate storage-way=3

	// a node's redis key MUST live longer than the event loop interval
	if r.cfg.SeedRegistry.EventLoopIntervalSec > r.cfg.SeedRegistry.NodeRegistryExp {
		err = ErrKeyExpMustBeGreater
		return
	}

	if redis.Client() == nil {
		err = ErrRedisNotAvail
		return
	}

	err = r.register() // hold here until we register the node
	return
}

func (r *SeedRegistry) register() (err error) {

	// determine who is first to boot for RaftBootstrap
	// use a distributed lock to wait until it's our turn to claim a name
	log.Info("waiting to acquire registry lock", logTag, "register")
	err = r.Lock()
	if err != nil {
		return
	}

	log.Info("registry lock acquired", logTag, "register")

	var address string
	address, err = r.GenerateNodeAddress()
	if err != nil {
		return
	}

	log.Info(fmt.Sprintf("using address: %s", address), logTag, "register")

	var nodename string = ""

	defer func() {
		if fErr := r.finalize(address, nodename); fErr != nil {
			err = fErr
		}
	}()

	nodename, err = r.GenerateNodeName(address)
	if err != nil {
		return
	}

	log.Info(fmt.Sprintf("using node name: %s", nodename), logTag, "register")
	return
}

func (r *SeedRegistry) GenerateNodeAddress() (address string, err error) {
	switch r.cfg.SeedRegistry.AddressWay {
	case AddressWayBindAddr:
		address = r.cfg.BindAddr
	case AddressWayPrivateIP:
		address, err = utils.GetPrivateIP()
	case AddressWayPublicIP:
		address, err = utils.GetPublicIP()
		return
	case AddressWayHostname:
		address, err = os.Hostname()
		return
	default:
		err = errors.New("invalid address-way")
	}
	if len(address) == 0 {
		err = ErrInvalidAddress
	}
	return
}

func (r *SeedRegistry) GenerateNodeName(addr string) (name string, err error) {

	// if we already have a node cached locally, resume
	ms := ReadMembers(r.NodesFilePath)
	for _, m := range ms {
		if m.Addr == addr {
			name = m.Name
			log.Info("found nodes.json file, resuming previous node name...", logTag, "register")
			return
		}
	}

	defer func() {
		name = fmt.Sprintf("%s%s", r.cfg.SeedRegistry.NodeNamePrefix, name)
	}()

	switch r.cfg.SeedRegistry.NodeNameWay {
	case NodeNameWayNodeName:
		name = r.cfg.NodeName
	case NodeNameWayPrivateIP:
		name, err = utils.GetPrivateIP()
		return
	case NodeNameWayPublicIP:
		name, err = utils.GetPublicIP()
		return
	case NodeNameWayHostname:
		name, err = os.Hostname()
		return
	case NodeNameWayUUID:
		name = utils.GenerateUUID4()
		return
	default:
		err = errors.New("invalid node-name-way")
	}
	if len(name) == 0 {
		err = ErrInvalidNodeName
	}
	return
}

func (r *SeedRegistry) finalize(address string, nodename string) (err error) {

	if len(address) == 0 {
		err = errors.New("empty address, check address-way is supported in your environment")
		return
	}
	if len(nodename) == 0 {
		err = errors.New("empty nodename, check node-name-way is supported in your environment")
		return
	}

	// get current registry from redis
	var registry []*Member
	registry, err = r.getRegistry()
	if err != nil {
		return
	}

	// raft leadership:
	// count == 0: we're the first node, assume leader
	// count == 1: maybe we've rebooted and are still the first node. check addr
	count := len(registry)
	if count == 0 || (count == 1 && registry[0].Addr == address) {
		r.cfg.RaftBootstrap = true
		log.Info(fmt.Sprintf("%s assuming raft leader", address), logTag, "register")
	}

	// set registration values
	r.NodeKey = fmt.Sprintf("%s:%s", address, nodename)
	r.cfg.BindAddr = address
	r.cfg.NodeName = nodename

	// register the node
	r.saveNode()

	// re-initialize members list with members from the registry
	r.cfg.Members = []string{}
	for _, member := range registry {
		r.cfg.Members = append(r.cfg.Members, fmt.Sprintf("%s:%d", member.Addr, r.cfg.BindPort))
	}

	// keep node updated and clean expiries
	go r.startEventLoop()

	// release the lock to other nodes
	err = r.Unlock()
	if err != nil {
		return
	}

	log.Info(fmt.Sprintf("seed member %s registered for %s", nodename, address), logTag, "register")
	log.Info("claim lock released", logTag, "register")
	return
}

func (r *SeedRegistry) getRegistry() (inventory []*Member, err error) {

	if redis.Client() == nil {
		err = ErrRedisNotAvail
		return
	}

	var keys map[string]string
	keys, err = redis.Client().HGetAll(r.ctx, r.cfg.SeedRegistry.NodesRegistryKey).Result()
	if err != nil && err != rdb.Nil {
		return
	}

	// in case we aren't running redis 7.4+, manually expire nodes that have gone away
	keep := make([]string, 0)
	trash := make([]string, 0)
	for node, ts_str := range keys {
		var ts int64
		ts, err = strconv.ParseInt(ts_str, 10, 64)
		if err != nil {
			return
		}
		// expired
		if ts < (time.Now().Unix() - r.cfg.SeedRegistry.NodeRegistryExp) {
			trash = append(trash, node)
		} else {
			keep = append(keep, node)
		}
	}
	if len(trash) > 0 {
		err = redis.Client().HDel(r.ctx, r.cfg.SeedRegistry.NodesRegistryKey, trash...).Err()
		if err != nil {
			return
		}
	}

	inventory = make([]*Member, 0)
	for _, key := range keep {
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			err = fmt.Errorf("invalid key: %s", key)
			return
		}
		inventory = append(inventory, &Member{
			Addr: parts[0],
			Name: parts[1],
		})
	}
	return
}

func (r *SeedRegistry) saveNode() (err error) {
	if redis.Client() == nil {
		err = ErrRedisNotAvail
		return
	}
	if len(r.NodeKey) > 0 {

		// set the registry entry
		err = redis.Client().HSet(
			r.ctx,
			r.cfg.SeedRegistry.NodesRegistryKey,
			r.NodeKey,
			fmt.Sprintf("%d", time.Now().Unix()),
		).Err()
		if err != nil {
			return
		}

		// if supported, place an EXP directly on the specific node in the table
		hexp_err := redis.Client().HExpire(
			r.ctx,
			r.cfg.SeedRegistry.NodesRegistryKey,
			time.Second*time.Duration(r.cfg.SeedRegistry.NodeRegistryExp),
			r.NodeKey,
		).Err()
		if hexp_err != nil {
			log.Debug(fmt.Sprintf("Could not use HEXPIRE directly. Ensure Redis 7.4+ is used and client is updated. %+v", hexp_err), logTag, "redis")
		}
	}
	return
}

func (r *SeedRegistry) removeNode() (err error) {
	if redis.Client() == nil {
		err = ErrRedisNotAvail
		return
	}
	if len(r.NodeKey) > 0 {
		err = redis.Client().HDel(r.ctx, r.cfg.SeedRegistry.NodesRegistryKey, r.NodeKey).Err()
	}
	return
}

func (r *SeedRegistry) startEventLoop() {
	for {
		select {
		// stop
		case <-r.term:
			log.Info("stopping event loop...", logTag, "register")
			return
		default:
			// bump node TTL
			err := r.saveNode()
			if err != nil {
				log.Error("r.saveNode():", err.Error(), logTag, "register")
				return
			}
			// getRegistry runs EXP on stale nodes
			_, err = r.getRegistry()
			if err != nil {
				log.Error("r.getRegistry():", err.Error(), logTag, "register")
				return
			}
			time.Sleep(time.Second * time.Duration(r.cfg.SeedRegistry.EventLoopIntervalSec))
		}
	}
}

func (r *SeedRegistry) Stop() (err error) {

	if !r.cfg.SeedRegistry.Enable {
		return
	}

	log.Info("stopping seed registry...", logTag, "register")

	// stop the event loop
	if r.term != nil {
		close(r.term)
	}

	// manually pull the node from redis
	err = r.removeNode()
	return
}

func (r *SeedRegistry) Lock() (err error) {
	if r.rdsync == nil {
		err = errors.New("redis sync is not available")
		return
	}
	r.lock = r.rdsync.NewMutex(
		r.cfg.SeedRegistry.LockKey,
		redsync.WithRetryDelay(time.Duration(r.cfg.SeedRegistry.LockLoopIntervalSec)),
		redsync.WithTries(r.cfg.SeedRegistry.MaxLockAttempts),
	)
	err = r.lock.Lock()
	return
}

func (r *SeedRegistry) Unlock() (err error) {
	_, err = r.lock.Unlock()
	return
}
