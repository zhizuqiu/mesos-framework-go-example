package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/mesos/mesos-go/api/v1/lib/resources"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/docker/docker/client"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
	"github.com/mesos/mesos-go/api/v1/lib/executor"
	"github.com/mesos/mesos-go/api/v1/lib/executor/calls"
	"github.com/mesos/mesos-go/api/v1/lib/executor/config"
	"github.com/mesos/mesos-go/api/v1/lib/executor/events"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpexec"
	"github.com/pborman/uuid"
)

const (
	apiPath     = "/api/v1/executor"
	httpTimeout = 10 * time.Second
)

var errMustAbort = errors.New("received abort signal from mesos, will attempt to re-subscribe")

var (
	h             bool
	ContainerName string
)

func init() {
	flag.BoolVar(&h, "h", false, "this help")

	flag.StringVar(&ContainerName, "container", "", "container name")
}

func usage() {
	fmt.Fprintf(os.Stderr, `vision executor version: vision/0.0.1
Options:
`)
	flag.PrintDefaults()
}

func main() {

	flag.Parse()

	if h {
		flag.Usage()
	}

	pid := os.Getpid()
	log.Println("PID: %d", pid)

	dir, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	log.Println("pwd:" +
		"")
	log.Println(dir)

	log.Println("env:")
	for _, e := range os.Environ() {
		log.Println(e)
	}

	log.Println("args:")
	log.Println("test os args", os.Args, len(os.Args))

	cfg, err := config.FromEnv()

	if err != nil {
		log.Fatal("failed to load configuration: " + err.Error())
	}
	log.Printf("configuration loaded: %+v", cfg)
	run(cfg)
	os.Exit(0)
}

func maybeReconnect(cfg config.Config) <-chan struct{} {
	if cfg.Checkpoint {
		return backoff.Notifier(1*time.Second, cfg.SubscriptionBackoffMax*3/4, nil)
	}
	return nil
}

func run(cfg config.Config) {
	var (
		apiURL = url.URL{
			Scheme: "http", // TODO(jdef) make this configurable
			Host:   cfg.AgentEndpoint,
			Path:   apiPath,
		}
		http = httpcli.New(
			httpcli.Endpoint(apiURL.String()),
			httpcli.Codec(codecs.ByMediaType[codecs.MediaTypeProtobuf]),
			httpcli.Do(httpcli.With(httpcli.Timeout(httpTimeout))),
		)
		callOptions = executor.CallOptions{
			calls.Framework(cfg.FrameworkID),
			calls.Executor(cfg.ExecutorID),
		}
		state = &internalState{
			cli: calls.SenderWith(
				httpexec.NewSender(http.Send),
				callOptions...,
			),
			unackedTasks:   make(map[mesos.TaskID]mesos.TaskInfo),
			unackedUpdates: make(map[string]executor.Call_Update),
			failedTasks:    make(map[mesos.TaskID]mesos.TaskStatus),
		}
		subscriber = calls.SenderWith(
			httpexec.NewSender(http.Send, httpcli.Close(true)),
			callOptions...,
		)
		shouldReconnect = maybeReconnect(cfg)
		disconnected    = time.Now()
		handler         = buildEventHandler(state)
	)
	for {
		func() {
			subscribe := calls.Subscribe(unacknowledgedTasks(state), unacknowledgedUpdates(state))

			log.Println("subscribing to agent for events..")
			resp, err := subscriber.Send(context.TODO(), calls.NonStreaming(subscribe))
			if resp != nil {
				defer resp.Close()
			}
			if err == nil {
				// we're officially connected, start decoding events
				err = eventLoop(state, resp, handler)
				disconnected = time.Now()
			}
			if err != nil && err != io.EOF {
				log.Println(err)
			} else {
				log.Println("disconnected")
			}
		}()
		if state.shouldQuit {
			log.Println("gracefully shutting down because we were told to")
			return
		}
		if !cfg.Checkpoint {
			log.Println("gracefully exiting because framework checkpointing is NOT enabled")
			return
		}
		if time.Now().Sub(disconnected) > cfg.RecoveryTimeout {
			log.Printf("failed to re-establish subscription with agent within %v, aborting", cfg.RecoveryTimeout)
			return
		}
		log.Println("waiting for reconnect timeout")
		<-shouldReconnect // wait for some amount of time before retrying subscription
	}
}

// unacknowledgedTasks generates the value of the UnacknowledgedTasks field of a Subscribe call.
func unacknowledgedTasks(state *internalState) (result []mesos.TaskInfo) {
	if n := len(state.unackedTasks); n > 0 {
		result = make([]mesos.TaskInfo, 0, n)
		for k := range state.unackedTasks {
			result = append(result, state.unackedTasks[k])
		}
	}
	return
}

// unacknowledgedUpdates generates the value of the UnacknowledgedUpdates field of a Subscribe call.
func unacknowledgedUpdates(state *internalState) (result []executor.Call_Update) {
	if n := len(state.unackedUpdates); n > 0 {
		result = make([]executor.Call_Update, 0, n)
		for k := range state.unackedUpdates {
			result = append(result, state.unackedUpdates[k])
		}
	}
	return
}

func eventLoop(state *internalState, decoder encoding.Decoder, h events.Handler) (err error) {
	log.Println("listening for events from agent...")
	ctx := context.TODO()
	for err == nil && !state.shouldQuit {
		// housekeeping
		sendFailedTasks(state)

		var e executor.Event
		if err = decoder.Decode(&e); err == nil {
			err = h.HandleEvent(ctx, &e)
		}
	}
	return err
}

func buildEventHandler(state *internalState) events.Handler {
	return events.HandlerFuncs{
		executor.Event_SUBSCRIBED: func(_ context.Context, e *executor.Event) error {
			log.Println("SUBSCRIBED")
			state.framework = e.Subscribed.FrameworkInfo
			state.executor = e.Subscribed.ExecutorInfo
			state.agent = e.Subscribed.AgentInfo
			return nil
		},
		executor.Event_LAUNCH: func(_ context.Context, e *executor.Event) error {
			launch(state, e.Launch.Task)
			return nil
		},
		executor.Event_KILL: func(_ context.Context, e *executor.Event) error {
			log.Println("kill")
			kill(state, e.Kill.TaskID)
			return nil
		},
		executor.Event_ACKNOWLEDGED: func(_ context.Context, e *executor.Event) error {
			log.Println("")
			log.Println("Event_ACKNOWLEDGED:")
			log.Println(e.Acknowledged.TaskID)
			delete(state.unackedTasks, e.Acknowledged.TaskID)
			delete(state.unackedUpdates, string(e.Acknowledged.UUID))
			return nil
		},
		executor.Event_MESSAGE: func(_ context.Context, e *executor.Event) error {
			log.Printf("MESSAGE: received %d bytes of message data", len(e.Message.Data))
			log.Printf(string(e.Message.Data))
			return nil
		},
		executor.Event_SHUTDOWN: func(_ context.Context, e *executor.Event) error {
			log.Println("SHUTDOWN received")
			state.shouldQuit = true
			return nil
		},
		executor.Event_ERROR: func(_ context.Context, e *executor.Event) error {
			log.Println("ERROR received")
			return errMustAbort
		},
	}.Otherwise(func(_ context.Context, e *executor.Event) error {
		log.Fatal("unexpected event", e)
		return nil
	})
}

func sendFailedTasks(state *internalState) {
	for taskID, status := range state.failedTasks {
		updateErr := update(state, status)
		if updateErr != nil {
			log.Printf("failed to send status update for task %s: %+v", taskID.Value, updateErr)
		} else {
			delete(state.failedTasks, taskID)
		}
	}
}

func max64(first int64, args ...int64) int64 {
	for _, v := range args {
		if first < v {
			first = v
		}
	}
	return first
}

var CPU_SHARES_PER_CPU = 1024
var MIN_CPU_SHARES = 2
var MIN_MEMORY = 4194304

func dockerRun(task mesos.TaskInfo) *container.ContainerCreateCreatedBody {
	ctx := context.Background()
	// cli, err := client.NewClientWithOpts(client.WithHost("tcp://112.125.89.9:2376"))
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Println(err)
		return nil
	}

	cpus := 0.0
	for _, r := range task.Resources {
		if cpu, ok := resources.CPUs(r); ok {
			cpus += cpu
		}
	}

	memory := uint64(0)
	for _, r := range task.Resources {
		if m, ok := resources.Memory(r); ok {
			memory += m
		}
	}

	resp, err := cli.ContainerCreate(
		context.Background(),
		&container.Config{
			Image: task.Container.Docker.Image,
			Tty:   true,
		},
		&container.HostConfig{
			Resources: container.Resources{
				// 1,048,576‬ = 1M 最小为4M = 4,194,304
				Memory: max64(int64(1048576*memory), int64(MIN_MEMORY)),
				// swap设定为Memory的2倍
				MemorySwap: max64(int64(1048576*memory), int64(MIN_MEMORY)) * 2,
				// 1024的倍数，最小值为2
				CPUShares: max64(int64(cpus*(float64(CPU_SHARES_PER_CPU))), int64(MIN_CPU_SHARES)),
			},
		},
		nil,
		ContainerName,
	)
	if err != nil {
		log.Println(err)
		return nil
	}

	log.Println(resp.ID)

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		log.Println(err)
		return nil
	}
	return &resp
}

func dockerInspect(id string) (*types.ContainerJSON, error) {
	ctx := context.Background()
	// cli, err := client.NewClientWithOpts(client.WithHost("tcp://112.125.89.9:2376"))
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	for {
		time.Sleep(500 * time.Millisecond)

		c, err := cli.ContainerInspect(ctx, id)
		if err == nil {
			return &c, nil
		}
	}
}

func launch(state *internalState, task mesos.TaskInfo) {
	state.unackedTasks[task.TaskID] = task

	b, _ := json.Marshal(task)
	log.Println(string(b))

	resp := dockerRun(task)

	i, _ := dockerInspect(resp.ID)
	is := []*types.ContainerJSON{i}
	ib, _ := json.Marshal(is)

	// send RUNNING
	status := newStatus(state, task.TaskID)
	status.State = mesos.TASK_RUNNING.Enum()
	status.Data = ib
	err := update(state, status)
	if err != nil {
		log.Printf("failed to send TASK_RUNNING for task %s: %+v", task.TaskID.Value, err)
		status.State = mesos.TASK_FAILED.Enum()
		status.Message = protoString(err.Error())
		state.failedTasks[task.TaskID] = status
		return
	}

	/*
		// send FINISHED
		status = newStatus(state, task.TaskID)
		status.State = mesos.TASK_FINISHED.Enum()
		err = update(state, status)
		if err != nil {
			log.Printf("failed to send TASK_FINISHED for task %s: %+v", task.TaskID.Value, err)
			status.State = mesos.TASK_FAILED.Enum()
			status.Message = protoString(err.Error())
			state.failedTasks[task.TaskID] = status
		}
	*/
}

func dockerKill() {
	ctx := context.Background()
	// cli, err := client.NewClientWithOpts(client.WithHost("tcp://112.125.89.9:2376"))
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("docker kill " + ContainerName)

	err = cli.ContainerKill(
		ctx,
		ContainerName,
		"SIGKILL",
	)
	if err != nil {
		log.Println(err)
		return
	}
}

func kill(state *internalState, taskId mesos.TaskID) {

	dockerKill()

	status := newStatus(state, taskId)

	// send FINISHED
	status = newStatus(state, taskId)
	status.State = mesos.TASK_FINISHED.Enum()
	err := update(state, status)
	if err != nil {
		log.Printf("failed to send TASK_FINISHED for task %s: %+v", taskId.Value, err)
		status.State = mesos.TASK_FAILED.Enum()
		status.Message = protoString(err.Error())
		state.failedTasks[taskId] = status
	}
}

// helper func to package strings up nicely for protobuf
// NOTE(jdef): if we need any more proto funcs like this, just import the
// proto package and use those.
func protoString(s string) *string { return &s }

func update(state *internalState, status mesos.TaskStatus) error {
	upd := calls.Update(status)
	resp, err := state.cli.Send(context.TODO(), calls.NonStreaming(upd))
	if resp != nil {
		resp.Close()
	}
	if err != nil {
		log.Printf("failed to send update: %+v", err)
		debugJSON(upd)
	} else {
		state.unackedUpdates[string(status.UUID)] = *upd.Update
	}
	return err
}

func newStatus(state *internalState, id mesos.TaskID) mesos.TaskStatus {
	return mesos.TaskStatus{
		TaskID:     id,
		Source:     mesos.SOURCE_EXECUTOR.Enum(),
		ExecutorID: &state.executor.ExecutorID,
		UUID:       []byte(uuid.NewRandom()),
	}
}

type internalState struct {
	cli            calls.Sender
	cfg            config.Config
	framework      mesos.FrameworkInfo
	executor       mesos.ExecutorInfo
	agent          mesos.AgentInfo
	unackedTasks   map[mesos.TaskID]mesos.TaskInfo
	unackedUpdates map[string]executor.Call_Update
	failedTasks    map[mesos.TaskID]mesos.TaskStatus // send updates for these as we can
	shouldQuit     bool
}
