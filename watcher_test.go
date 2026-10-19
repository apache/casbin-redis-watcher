package rediswatcher_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/casbin/casbin/v3"
	"github.com/casbin/casbin/v3/persist"
	rediswatcher "github.com/casbin/redis-watcher/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestWatcherCloseDoesNotPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer client.Close()
	channel := "/casbin-close-" + uuid.NewString()
	observer := client.Subscribe(ctx, channel)
	defer observer.Close()
	if _, err := observer.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	messages := observer.Channel()
	pubClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	subClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	watcher, err := rediswatcher.NewWatcher("", rediswatcher.WatcherOptions{
		Channel: channel, PubClient: pubClient, SubClient: subClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			watcher.Close()
		}
	}()
	updates := make(chan string, 1)
	survivorID := uuid.NewString()
	survivor, err := rediswatcher.NewWatcher("127.0.0.1:6379", rediswatcher.WatcherOptions{
		Channel: channel,
		LocalID: survivorID,
		OptionalUpdateCallback: func(message string) {
			updates <- message
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer survivor.Close()
	waitForWatcherCondition(t, ctx, func() bool {
		counts, err := client.PubSubNumSub(ctx, channel).Result()
		return err == nil && counts[channel] == 3
	})
	watcher.Close()
	closed = true
	waitForWatcherCondition(t, ctx, func() bool {
		counts, err := client.PubSubNumSub(ctx, channel).Result()
		return err == nil && counts[channel] == 2 &&
			errors.Is(pubClient.Ping(ctx).Err(), redis.ErrClosed) &&
			errors.Is(subClient.Ping(ctx).Err(), redis.ErrClosed)
	})
	if err := survivor.Update(); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-messages:
		var update rediswatcher.MSG
		if err := json.Unmarshal([]byte(message.Payload), &update); err != nil {
			t.Fatalf("Close published a non-JSON message: %q", message.Payload)
		}
		if update.Method != rediswatcher.Update || update.ID != survivorID {
			t.Fatalf("unexpected message after Close: %q", message.Payload)
		}
	case <-ctx.Done():
		t.Fatal("surviving watcher did not publish an update")
	}
	select {
	case <-updates:
	case <-ctx.Done():
		t.Fatal("surviving watcher did not receive an update")
	}
}

func TestWatcherCloseIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pubClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	subClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	watcher, err := rediswatcher.NewWatcher("", rediswatcher.WatcherOptions{
		Channel: "/casbin-close-" + uuid.NewString(), PubClient: pubClient, SubClient: subClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			watcher.Close()
		}()
	}
	wg.Wait()
	waitForWatcherCondition(t, ctx, func() bool {
		return errors.Is(pubClient.Ping(ctx).Err(), redis.ErrClosed) &&
			errors.Is(subClient.Ping(ctx).Err(), redis.ErrClosed)
	})
}

func waitForWatcherCondition(t *testing.T, ctx context.Context, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for watcher lifecycle condition")
		case <-ticker.C:
		}
	}
}

func initWatcherWithOptions(t *testing.T, wo rediswatcher.WatcherOptions, cluster ...bool) (*casbin.Enforcer, *rediswatcher.Watcher) {
	var (
		w   persist.Watcher
		err error
	)
	if len(cluster) > 0 && cluster[0] {
		w, err = rediswatcher.NewWatcherWithCluster("127.0.0.1:6379,127.0.0.1:6379,127.0.0.1:6379", wo)
	} else {
		w, err = rediswatcher.NewWatcher("127.0.0.1:6379", wo)
	}
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	e, err := casbin.NewEnforcer("examples/rbac_model.conf", "examples/rbac_policy.csv")
	if err != nil {
		t.Fatalf("Failed to create enforcer: %v", err)
	}
	_ = e.SetWatcher(w)
	_ = w.SetUpdateCallback(func(s string) {
		t.Log(s)
		rediswatcher.DefaultUpdateCallback(e)(s)
	})
	return e, w.(*rediswatcher.Watcher)
}

func initWatcher(t *testing.T, cluster ...bool) (*casbin.Enforcer, *rediswatcher.Watcher) {
	return initWatcherWithOptions(t, rediswatcher.WatcherOptions{}, cluster...)
}

func TestWatcher(t *testing.T) {
	_, w := initWatcher(t)
	_ = w.SetUpdateCallback(func(s string) {
		fmt.Println(s)
	})
	_ = w.Update()
	w.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestWatcherWithIngnoreSelfFalse(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		LocalID: uuid.New().String(),
	}
	_, w := initWatcherWithOptions(t, wo)
	_ = w.SetUpdateCallback(func(s string) {
		msg := &rediswatcher.MSG{}
		_ = json.Unmarshal([]byte(s), msg)
		if msg.ID != wo.LocalID {
			t.Fatalf("instance ID should be %s instead of %s", wo.LocalID, msg.ID)
		}
	})

	_ = w.Update()
	time.Sleep(time.Millisecond * 500)
	w.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestWatcherWithIgnoreSelfTrue(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	_, w := initWatcherWithOptions(t, wo)

	_ = w.SetUpdateCallback(func(s string) {
		t.Fatalf("This callback should not be called when IgnoreSelf is set true.")
	})

	_ = w.Update()
	time.Sleep(time.Millisecond * 500)
	w.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdate(t *testing.T) {
	_, w := initWatcher(t)
	_ = w.SetUpdateCallback(func(s string) {
		msgStruct := &rediswatcher.MSG{}

		err := msgStruct.UnmarshalBinary([]byte(s))
		if err != nil {
			t.Error(err)
			return
		}
		if msgStruct.Method != "Update" {
			t.Errorf("Method should be Update instead of %s", msgStruct.Method)
		}
	})
	_ = w.Update()
	w.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateForAddPolicy(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_, _ = e.AddPolicy("alice", "book1", "write")
	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "AddPolicy")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateForRemovePolicy(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_, _ = e.RemovePolicy("alice", "data1", "read")
	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "RemovePolicy")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateForRemoveFilteredPolicy(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_, _ = e.RemoveFilteredPolicy(1, "data1", "read")
	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "RemoveFilteredPolicy")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateSavePolicy(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_ = e.SavePolicy()
	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "SavePolicy")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateForAddPolicies(t *testing.T) {
	rules := [][]string{
		{"jack", "data4", "read"},
		{"katy", "data4", "write"},
		{"leyo", "data4", "read"},
		{"ham", "data4", "write"},
	}

	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_, _ = e.AddPolicies(rules)
	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "AddPolicies")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateForRemovePolicies(t *testing.T) {
	rules := [][]string{
		{"jack", "data4", "read"},
		{"katy", "data4", "write"},
		{"leyo", "data4", "read"},
		{"ham", "data4", "write"},
	}

	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_, _ = e.RemovePolicies(rules)
	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "RemovePolicies")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateForUpdatePolicy(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_, _ = e.UpdatePolicy([]string{"alice", "data1", "read"}, []string{"alice", "data1", "write"})

	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "UpdatePolicy")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestUpdateForUpdatePolicies(t *testing.T) {
	wo := rediswatcher.WatcherOptions{
		IgnoreSelf: true,
	}
	e, w := initWatcherWithOptions(t, wo)
	e2, w2 := initWatcherWithOptions(t, wo)

	time.Sleep(time.Millisecond * 500)
	_, _ = e.UpdatePolicies([][]string{{"alice", "data1", "read"}}, [][]string{{"alice", "data1", "write"}})

	time.Sleep(time.Millisecond * 500)
	ePolicy, _ := e.GetPolicy()
	e2Policy, _ := e2.GetPolicy()
	if !reflect.DeepEqual(e2Policy, ePolicy) {
		t.Log("Method", "UpdatePolicies")
		t.Log("e.policy", ePolicy)
		t.Log("e2.policy", e2Policy)
		t.Error("These two enforcers' policies should be equal")
	}

	w.Close()
	w2.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestNewWatcherWithClusterUsingExistingClient(t *testing.T) {
	// Create a regular Redis client that implements UniversalClient interface
	// In production, this would be a cluster client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	// Use the existing client for both pub and sub
	// This demonstrates that UniversalClient type works with any Redis client type
	wo := rediswatcher.WatcherOptions{
		SubClient: redisClient,
		PubClient: redisClient,
	}

	// Initialize watcher with existing client
	// Pass empty string for addrs since we're using existing clients
	// When SubClient and PubClient are provided, the addrs parameter is ignored
	w, err := rediswatcher.NewWatcherWithCluster("", wo)
	if err != nil {
		t.Fatalf("Failed to create watcher with existing client: %v", err)
	}
	watcher := w.(*rediswatcher.Watcher)

	// Verify the watcher works
	_ = watcher.SetUpdateCallback(func(s string) {
		t.Log("Received update:", s)
	})

	_ = watcher.Update()
	time.Sleep(time.Millisecond * 500)

	watcher.Close()
	time.Sleep(time.Millisecond * 500)
}

func TestNewWatcherWithExistingClient(t *testing.T) {
	// Create a regular Redis client externally
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	// Use the existing client for both pub and sub
	wo := rediswatcher.WatcherOptions{
		SubClient: redisClient,
		PubClient: redisClient,
	}

	// Initialize watcher with existing client
	// Pass empty string for addr since we're using existing clients
	// When SubClient and PubClient are provided, the addr parameter is ignored
	w, err := rediswatcher.NewWatcher("", wo)
	if err != nil {
		t.Fatalf("Failed to create watcher with existing client: %v", err)
	}
	watcher := w.(*rediswatcher.Watcher)

	// Verify the watcher works
	_ = watcher.SetUpdateCallback(func(s string) {
		t.Log("Received update:", s)
	})

	_ = watcher.Update()
	time.Sleep(time.Millisecond * 500)

	watcher.Close()
	time.Sleep(time.Millisecond * 500)
}
