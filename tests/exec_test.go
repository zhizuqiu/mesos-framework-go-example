package tests

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"testing"
)

func TestCmd(t *testing.T) {
	cmd := exec.Command("while.exe", "-s", "true")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	// err := cmd.Run()
	err := cmd.Start()
	if err != nil {
		fmt.Println("cmd.Output: ", err)
	}
	fmt.Println(cmd.Process.Pid)

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
		log.Println("Sys:", ps.Sys())
		log.Println("SystemTime:", ps.SystemTime())
		log.Println("UserTime:", ps.UserTime())
		log.Println("SysUsage:", ps.SysUsage())
		log.Println("Exited:", ps.Exited())
		log.Println("Success:", ps.Success())
	}(cmd)

	fmt.Println("wait....")

	wg.Wait()

	fmt.Println("wait....done")

	/*	state, err := cmd.Process.Wait()
		if err != nil {
			fmt.Println("Wait err: ", err)
		}
		fmt.Println(state)
	*/
}

func TestCmd2(t *testing.T) {
	cmd := exec.Command("while.exe", "-s", "true")

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s\n", err)
	}
	fmt.Printf("combined out:\n%s\n", string(out))

	fmt.Println(cmd.ProcessState)

}
