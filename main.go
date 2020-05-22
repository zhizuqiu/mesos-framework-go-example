package main

import (
	"log"
	"mesos-framework-go-example/app"
	"time"
)

func main() {

	config := app.Config{
		User: "root",
		// 名字
		Name: "test-framework-go",
		// 角色
		Role: "*",
		// Mesos 的scheduler接口
		Url: "http://localhost:5050/api/v1/scheduler",
		// 用于标识Framework运行的机器，可选
		Hostname: "localhost",
		// 如果你开发的Framework有Web页面的话，这个配置可以标识Framework的UI地址，就能Mesos页面中跳转到此地址
		WebUiURL: "http://localhost:8080/",
		// 故障恢复的超时时间，Mesos运行你在此时间段内，使用相同的Framework Id重新注册
		FailoverTimeout: 0,
		Checkpoint:      false,
		Timeout:         10 * time.Second,
		VerboseLog:      false,
	}

	// 这里简单地提出了一些任务的配置，例如cpu、内存、镜像名字、下发的数量等
	// 在实际开发中，你可以更灵活地自定义这些配置
	state := app.State{
		Cfg:          config,
		TaskLaunched: 0,
		TotalTasks:   1,
		Cpu:          0.1,
		Mem:          128,
		Images:       "nginx",
	}

	err := app.DockerRun(config, state)
	if err != nil {
		log.Println(err)
	}

}
