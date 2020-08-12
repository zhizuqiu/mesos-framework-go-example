package docker

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

type RunOptions struct {
	Privileged        bool
	CpuShares         int64
	CpuQuota          int64
	MemoryReservation int64
	Memory            int64
	OomScoreAdj       int64
	Env               map[string]string
	Volumes           []string
	VolumeDriver      string
	Network           string
	Hostname          string
	Dns               []string
	DnsSearch         []string
	DnsOpt            []string
	PortMappings      []PortMapping
	Devices           []Device
	Entrypoint        string
	Name              string
	AdditionalOptions []string
	Image             string
	Arguments         []string
}

type PortMapping struct {
	HostPort      int64
	ContainerPort int64
	Protocol      string
}

type Device struct {
	HostPath      string
	ContainerPath string
	Access        struct {
		Read  bool
		Write bool
		Mknod bool
	}
}

func Run(options RunOptions) (*exec.Cmd, error) {
	argv := make([]string, 0)
	argv = append(argv, "-H")
	argv = append(argv, "unix:///var/run/docker.sock")
	argv = append(argv, "run")

	if options.Privileged {
		argv = append(argv, "--privileged")
	}

	if options.CpuShares > 0 {
		argv = append(argv, "--cpu-shares")
		argv = append(argv, strconv.FormatInt(options.CpuShares, 10))
	}

	if options.CpuQuota > 0 {
		argv = append(argv, "--cpu-quota")
		argv = append(argv, strconv.FormatInt(options.CpuQuota, 10))
	}

	if options.MemoryReservation > 0 {
		argv = append(argv, "--memory-reservation")
		argv = append(argv, strconv.FormatInt(options.MemoryReservation, 10))
	}

	if options.Memory > 0 {
		argv = append(argv, "--memory")
		argv = append(argv, strconv.FormatInt(options.Memory, 10))
	}

	if options.OomScoreAdj > 0 {
		argv = append(argv, "--oom-score-adj")
		argv = append(argv, strconv.FormatInt(options.OomScoreAdj, 10))
	}

	if options.Env != nil {
		for k, v := range options.Env {
			argv = append(argv, "-e")
			argv = append(argv, k+"="+v)
		}
	}

	if options.Volumes != nil {
		for _, v := range options.Volumes {
			argv = append(argv, "-v")
			argv = append(argv, v)
		}
	}

	if options.VolumeDriver != "" {
		argv = append(argv, "--volume-driver="+options.VolumeDriver)
	}

	if options.Network != "" {
		argv = append(argv, "--net")
		argv = append(argv, options.Network)

		// TODO docker在1.9.0版本以上才支持user模式 https://github.com/apache/mesos/blob/master/src/docker/docker.cpp#L1122
		// TODO 如果是host网络模式，并且dns不为空，docker版本必须在1.12.0以上 https://github.com/apache/mesos/blob/master/src/docker/docker.cpp#L1132
	}

	if options.Dns != nil {
		for _, dns := range options.Dns {
			argv = append(argv, "--dns")
			argv = append(argv, dns)
		}
	}

	if options.DnsSearch != nil {
		for _, search := range options.DnsSearch {
			argv = append(argv, "--dns-search")
			argv = append(argv, search)
		}
	}

	if options.DnsOpt != nil {
		// TODO 校验docker版本是否>=1.9.0 https://github.com/apache/mesos/blob/master/src/docker/docker.cpp#L1153

		for _, opt := range options.DnsOpt {
			argv = append(argv, "--dns-opt")
			argv = append(argv, opt)
		}
	}

	if options.Hostname != "" {
		argv = append(argv, "--hostname")
		argv = append(argv, options.Hostname)
	}

	if options.PortMappings != nil {
		for _, mapping := range options.PortMappings {
			argv = append(argv, "-p")
			portMapping := strconv.FormatInt(mapping.HostPort, 10) + ":" + strconv.FormatInt(mapping.ContainerPort, 10)
			if mapping.Protocol != "" {
				portMapping += "/" + strings.ToLower(mapping.Protocol)
			}
			argv = append(argv, portMapping)
		}
	}

	if options.Devices != nil {
		for _, device := range options.Devices {
			if !path.IsAbs(device.HostPath) {
				return nil, errors.New("Device path '" + device.HostPath + "' is not an absolute path")
			}

			permissions := ""
			if device.Access.Read {
				permissions += "r"
			}
			if device.Access.Write {
				permissions += "w"
			}
			if device.Access.Mknod {
				permissions += "m"
			}
			if permissions == "" {
				return nil, errors.New("At least one access required for --devices: none specified for '" + device.HostPath + "'")
			}
			argv = append(argv, "--device="+device.HostPath+":"+device.ContainerPath+":"+permissions)
		}
	}

	if options.Entrypoint != "" {
		argv = append(argv, "--entrypoint")
		argv = append(argv, options.Entrypoint)
	}

	if options.Name != "" {
		argv = append(argv, "--name")
		argv = append(argv, options.Name)
	}

	if options.AdditionalOptions != nil {
		for _, option := range options.AdditionalOptions {
			argv = append(argv, option)
		}
	}

	if options.Image == "" {
		return nil, errors.New("none specified for 'image'")
	}
	argv = append(argv, options.Image)

	if options.Arguments != nil {
		for _, argument := range options.Arguments {
			argv = append(argv, argument)
		}
	}

	cmd := exec.Command("docker", argv...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	return cmd, nil
}

type ChanByteResult struct {
	B   []byte
	Err error
}

func Inspect(containerName string, chanByteResult chan ChanByteResult, quit chan bool) {
	argv := make([]string, 0)
	argv = append(argv, "-H")
	argv = append(argv, "unix:///var/run/docker.sock")
	argv = append(argv, "inspect")
	argv = append(argv, "--type=container")
	argv = append(argv, containerName)

	cmd := exec.Command("docker", argv...)

	defer close(quit)
	go func(quit chan bool) {
		q := <-quit
		if q {
			log.Println("cmd.Process.Kill()")
			defer cmd.Process.Kill()
		}
	}(quit)

	b, err := cmd.CombinedOutput()

	ps := cmd.ProcessState
	log.Println("docker inspect state:")
	log.Println("Pid:", ps.Pid(), ps.String())
	log.Println("ExitCode:", ps.ExitCode())
	log.Println("SystemTime:", ps.SystemTime())
	log.Println("UserTime:", ps.UserTime())
	log.Println("Exited:", ps.Exited())
	log.Println("Success:", ps.Success())

	if err != nil {
		chanByteResult <- ChanByteResult{
			B:   nil,
			Err: errors.New(string(b)),
		}
	} else {
		chanByteResult <- ChanByteResult{
			B:   b,
			Err: nil,
		}
	}
}

func Kill(signal int, containerName string) (*exec.Cmd, error) {
	argv := make([]string, 0)
	argv = append(argv, "-H")
	argv = append(argv, "unix:///var/run/docker.sock")
	argv = append(argv, "kill")
	argv = append(argv, "--signal="+strconv.Itoa(signal))
	argv = append(argv, containerName)

	cmd := exec.Command("docker", argv...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	return cmd, nil
}
