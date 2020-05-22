package app

import (
	"context"
	"fmt"
	mesos "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/encoding/codecs"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/callrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/eventrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/store"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/resources"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	User             string
	Name             string
	Role             string
	Url              string
	WebUiURL         string
	FailoverTimeout  time.Duration
	Checkpoint       bool
	Principal        string
	Hostname         string
	GpuClusterCompat bool
	Timeout          time.Duration
	VerboseLog       bool
}

type State struct {
	Cfg          Config
	Cli          calls.Caller
	Shutdown     func()
	TaskLaunched int
	TotalTasks   int
	Cpu          float64
	Mem          float64
	Images       string
}

func DockerRun(cfg Config, state State) error {
	fidStore := store.DecorateSingleton(
		store.NewInMemorySingleton(),
		store.DoSet().AndThen(func(_ store.Setter, v string, _ error) error {
			log.Println("FrameworkID", v)
			return nil
		}))

	state.Cli = buildHTTPSched(cfg)

	state.Cli = callrules.New(
		callrules.WithFrameworkID(store.GetIgnoreErrors(fidStore)),
	).Caller(state.Cli)

	ctx, cancel := context.WithCancel(context.Background())
	state.Shutdown = cancel

	err := controller.Run(
		ctx,
		buildFrameworkInfo(cfg),
		state.Cli,
		controller.WithEventHandler(buildEventHandler(cfg, state, fidStore)),
		controller.WithFrameworkID(store.GetIgnoreErrors(fidStore)),
		controller.WithRegistrationTokens(
			backoff.Notifier(RegistrationMinBackoff, RegistrationMaxBackoff, ctx.Done()),
		),
		controller.WithSubscriptionTerminated(func(err error) {
			if err != nil {
				if err != io.EOF {
					log.Println(err)
				}
				if _, ok := err.(StateError); ok {
					state.Shutdown()
				}
				return
			}
			log.Println("disconnected")
		}),
	)
	if err != nil {
		return err
	}
	return nil
}

func buildFrameworkInfo(cfg Config) *mesos.FrameworkInfo {
	failoverTimeout := cfg.FailoverTimeout.Seconds()
	frameworkInfo := &mesos.FrameworkInfo{
		User:       cfg.User,
		Name:       cfg.Name,
		Checkpoint: &cfg.Checkpoint,
		Capabilities: []mesos.FrameworkInfo_Capability{
			{Type: mesos.FrameworkInfo_Capability_RESERVATION_REFINEMENT},
		},
	}
	if cfg.FailoverTimeout > 0 {
		frameworkInfo.FailoverTimeout = &failoverTimeout
	}
	if cfg.Role != "" {
		frameworkInfo.Role = &cfg.Role
	}
	if cfg.Principal != "" {
		frameworkInfo.Principal = &cfg.Principal
	}
	if cfg.Hostname != "" {
		frameworkInfo.Hostname = &cfg.Hostname
	}
	if cfg.WebUiURL != "" {
		frameworkInfo.WebUiURL = &cfg.WebUiURL
	}
	if cfg.GpuClusterCompat {
		frameworkInfo.Capabilities = append(frameworkInfo.Capabilities,
			mesos.FrameworkInfo_Capability{Type: mesos.FrameworkInfo_Capability_GPU_RESOURCES},
		)
	}
	return frameworkInfo
}

type codec struct{ encoding.Codec }

func (c *codec) Set(value string) error {
	v := strings.ToLower(value)
	for _, codec := range codecs.ByMediaType {
		if v == codec.Name {
			c.Codec = codec
			return nil
		}
	}
	return fmt.Errorf("bad codec %q", value)
}

func buildHTTPSched(cfg Config) calls.Caller {
	var authConfigOpt httpcli.ConfigOpt

	cli := httpcli.New(
		httpcli.Endpoint(cfg.Url),
		httpcli.Codec(codec{Codec: codecs.ByMediaType[codecs.MediaTypeProtobuf]}.Codec),
		httpcli.Do(httpcli.With(
			authConfigOpt,
			httpcli.Timeout(cfg.Timeout),
		)),
	)

	return httpsched.NewCaller(cli, httpsched.Listener(func(n httpsched.Notification) {
		if cfg.VerboseLog {
			log.Printf("scheduler client notification: %+v", n)
		}
	}))
}

// buildEventHandler generates and returns a handler to process events received from the subscription.
func buildEventHandler(cfg Config, state State, fidStore store.Singleton) events.Handler {
	// disable brief logs when verbose logs are enabled (there's no sense logging twice!)
	logger := controller.LogEvents(nil).Unless(cfg.VerboseLog)
	return eventrules.New(
		logAllEvents().If(cfg.VerboseLog),
		controller.LiftErrors().DropOnError(),
	).Handle(events.Handlers{
		scheduler.Event_FAILURE: logger.HandleF(failure),
		scheduler.Event_OFFERS:  trackOffersReceived().HandleF(resourceOffers(cfg, state)),
		scheduler.Event_UPDATE:  controller.AckStatusUpdates(state.Cli).AndThen().HandleF(statusUpdate(cfg)),
		scheduler.Event_SUBSCRIBED: eventrules.New(
			logger,
			controller.TrackSubscription(fidStore, cfg.FailoverTimeout),
		),
	}.Otherwise(logger.HandleEvent))
}

func resourceOffers(cfg Config, state State) events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		var (
			offers                 = e.GetOffers().GetOffers()
			callOption             = calls.RefuseSecondsWithJitter(rand.New(rand.NewSource(time.Now().Unix())), 10*time.Second)
			tasksLaunchedThisCycle = 0
			offersDeclined         = 0
		)

		// 如果完成了任务的下发，可以主动的suppress offer
		if state.TaskLaunched >= state.TotalTasks {
			err := calls.CallNoData(ctx, state.Cli, calls.Suppress())
			if err != nil {
				log.Println("failed to suppress offer")
			} else {
				log.Println("suppress offer...")
			}
		}

		for i := range offers {
			var (
				remaining = mesos.Resources(offers[i].Resources)
				tasks     = []mesos.TaskInfo{}
			)

			if cfg.VerboseLog {
				log.Println("received offer id '" + offers[i].ID.Value +
					"' with resources " + remaining.String())
			}

			flattened := remaining.ToUnreserved()

			var taskWantsResources = mesos.Resources{}
			taskWantsResources.Add(
				resources.NewCPUs(state.Cpu).Resource,
				resources.NewMemory(state.Mem).Resource,
			)

			for state.TaskLaunched < state.TotalTasks && resources.ContainsAll(flattened, taskWantsResources) {
				found := func() mesos.Resources {
					if cfg.Role == "*" {
						return resources.Find(taskWantsResources, remaining...)
					}
					reservation := mesos.Resource_ReservationInfo{
						Type: mesos.Resource_ReservationInfo_STATIC.Enum(),
						Role: &cfg.Role,
					}
					return resources.Find(taskWantsResources.PushReservation(reservation))
				}()

				if len(found) == 0 {
					panic("illegal state: failed to find the resources that were supposedly contained")
				}

				state.TaskLaunched++
				taskID := state.TaskLaunched

				if cfg.VerboseLog {
					log.Println("launching task " + strconv.Itoa(taskID) + " using offer " + offers[i].ID.Value)
				}

				docker := &mesos.ContainerInfo_DockerInfo{
					Image:   state.Images,
					Network: mesos.ContainerInfo_DockerInfo_BRIDGE.Enum(),
				}

				container := &mesos.ContainerInfo{
					Type:     mesos.ContainerInfo_DOCKER.Enum(),
					Volumes:  nil,
					Hostname: nil,
					Docker:   docker,
				}

				isShell := false

				task := mesos.TaskInfo{
					TaskID:    mesos.TaskID{Value: strconv.Itoa(taskID)},
					AgentID:   offers[i].AgentID,
					Resources: found,
					Command: &mesos.CommandInfo{
						Shell: &isShell,
					},
					Container: container,
				}
				task.Name = "Task " + task.TaskID.Value
				tasks = append(tasks, task)

				remaining.Subtract(task.Resources...)
				flattened = remaining.ToUnreserved()
			}

			// build Accept call to launch all of the tasks we've assembled
			accept := calls.Accept(
				calls.OfferOperations{calls.OpLaunch(tasks...)}.WithOffers(offers[i].ID),
			).With(callOption)

			// send Accept call to mesos
			err := calls.CallNoData(ctx, state.Cli, accept)
			if err != nil {
				log.Printf("failed to launch tasks: %+v", err)
			} else {
				if n := len(tasks); n > 0 {
					tasksLaunchedThisCycle += n
				} else {
					offersDeclined++
				}
			}
		}

		if tasksLaunchedThisCycle == 0 {
			log.Println("zero tasks launched this cycle")
		}
		return nil
	}
}

func statusUpdate(cfg Config) events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) error {
		s := e.GetUpdate().GetStatus()
		if cfg.VerboseLog {
			msg := "Task " + s.TaskID.Value + " is in state " + s.GetState().String()
			if m := s.GetMessage(); m != "" {
				msg += " with message '" + m + "'"
			}
			log.Println(msg)
		}

		switch st := s.GetState(); st {
		case mesos.TASK_FINISHED:
			log.Println(s.TaskID.Value + " FINISHED")
		case mesos.TASK_LOST, mesos.TASK_KILLED, mesos.TASK_FAILED, mesos.TASK_ERROR:
			log.Println(s.TaskID.Value + " " + st.String())
		default:
			log.Println(s.TaskID.Value + " " + st.String())
		}
		return nil
	}
}

func trackOffersReceived() eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
		if err == nil {
			log.Println(len(e.GetOffers().GetOffers()))
		}
		return chain(ctx, e, err)
	}
}

func failure(_ context.Context, e *scheduler.Event) error {
	var (
		f              = e.GetFailure()
		eid, aid, stat = f.ExecutorID, f.AgentID, f.Status
	)
	if eid != nil {
		// executor failed..
		msg := "executor '" + eid.Value + "' terminated"
		if aid != nil {
			msg += " on agent '" + aid.Value + "'"
		}
		if stat != nil {
			msg += " with status=" + strconv.Itoa(int(*stat))
		}
		log.Println(msg)
	} else if aid != nil {
		// agent failed..
		log.Println("agent '" + aid.Value + "' terminated")
	}
	return nil
}

// logAllEvents logs every observed event; this is somewhat expensive to do
func logAllEvents() eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, ch eventrules.Chain) (context.Context, *scheduler.Event, error) {
		log.Printf("%+v\n", *e)
		return ch(ctx, e, err)
	}
}

var (
	RegistrationMinBackoff = 1 * time.Second
	RegistrationMaxBackoff = 15 * time.Second
)

// StateError is returned when the system encounters an unresolvable state transition error and
// should likely exit.
type StateError string

func (err StateError) Error() string { return string(err) }
