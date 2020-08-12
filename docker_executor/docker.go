package docker_executor

import (
	"errors"
	mesos "github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/executor/config"
	"github.com/mesos/mesos-go/api/v1/lib/resources"
	"log"
	"mesos-framework-go-example/docker"
	"mesos-framework-go-example/tools"
	"os/exec"
	"sync"
	"time"
)

var CpuSharesPerCpu = 1024
var MinCpuShares = 2
var MinMemory = 4194304

func DockerRun(cfg config.Config, task mesos.TaskInfo, containerName string, exitCall func(taskState *mesos.TaskState, mess *string)) error {

	if task.Container.Docker == nil {
		return errors.New("No docker info found in container info")
	}

	dockerInfo := task.Container.Docker

	resourceRequests := append(task.Resources, task.Executor.Resources...)

	// todo task.Limits

	// cpus
	cpus := 0.0
	for _, r := range resourceRequests {
		if cpu, ok := resources.CPUs(r); ok {
			cpus += cpu
		}
	}

	// mem
	memory := uint64(0)
	for _, r := range resourceRequests {
		if m, ok := resources.Memory(r); ok {
			memory += m
		}
	}

	// env
	env := make(map[string]string)
	if task.Executor != nil && task.Executor.Command != nil && task.Executor.Command.Environment != nil {
		for _, e := range task.Executor.Command.Environment.Variables {
			env[e.Name] = *e.Value
		}
	}

	// Binds
	binds := make([]string, 0)
	binds = append(binds, cfg.Sandbox+":/mnt/mesos/sandbox")

	runOptions := docker.RunOptions{
		Privileged: *dockerInfo.Privileged,
		// 1024的倍数，最小值为2
		CpuShares:         tools.Max64(int64(cpus*(float64(CpuSharesPerCpu))), int64(MinCpuShares)),
		CpuQuota:          0,
		MemoryReservation: 0,
		// 1,048,576‬ = 1M 最小为4M = 4,194,304
		Memory:            tools.Max64(int64(1048576*memory), int64(MinMemory)),
		OomScoreAdj:       0,
		Env:               env,
		Volumes:           binds,
		VolumeDriver:      "",
		Network:           "bridge",
		Hostname:          "",
		Dns:               nil,
		DnsSearch:         nil,
		DnsOpt:            nil,
		PortMappings:      nil,
		Devices:           nil,
		Entrypoint:        "",
		Name:              containerName,
		AdditionalOptions: nil,
		Image:             task.Container.Docker.Image,
	}

	if task.Executor != nil && task.Executor.Command != nil && task.Executor.Command.Arguments != nil {
		runOptions.Arguments = task.Executor.Command.Arguments
	}

	cmd, err := docker.Run(runOptions)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(cmd *exec.Cmd) {
		defer wg.Done()
		err := cmd.Wait()
		if err != nil {
			log.Println(err)
		}
		ps := cmd.ProcessState
		log.Println("Pid:", ps.Pid(), ps.String())
		log.Println("ExitCode:", ps.ExitCode())
		log.Println("SystemTime:", ps.SystemTime())
		log.Println("UserTime:", ps.UserTime())
		log.Println("Exited:", ps.Exited())
		log.Println("Success:", ps.Success())

		switch ps.ExitCode() {
		case 0:
			exitCall(mesos.TASK_FINISHED.Enum(), tools.StringPtr(ps.String()))
		case 9 + 128:
			exitCall(mesos.TASK_KILLED.Enum(), tools.StringPtr(ps.String()))
		default:
			exitCall(mesos.TASK_FAILED.Enum(), tools.StringPtr(ps.String()))
		}
	}(cmd)

	return nil
}

var DockerInspectTimeout = 5 * time.Second
var DockerInspectDelay = 500 * time.Millisecond

func DockerInspect(containerName string) []byte {
	for {
		b, err := dockerInspect(containerName, DockerInspectTimeout)
		if err != nil {
			log.Println("DockerInspect err:")
			log.Println(err)
		} else {
			return b
		}
		time.Sleep(DockerInspectDelay)
	}
}

func dockerInspect(containerName string, timeout time.Duration) ([]byte, error) {
	chanByteResult := make(chan docker.ChanByteResult)
	quit := make(chan bool)

	go docker.Inspect(containerName, chanByteResult, quit)

	select {
	case ch := <-chanByteResult:
		return ch.B, ch.Err
	case <-time.After(timeout):
		quit <- true
		return nil, errors.New("docker inspect timeout")
	}
}

func DockerKill(containerName string) {
	docker.Kill(9, containerName)
}
