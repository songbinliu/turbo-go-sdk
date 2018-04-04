package wsocket

import (
	"fmt"
	"testing"
	"time"
)

func TestNewConnectionConfig(t *testing.T) {
	hosts := []string{
		"https://127.0.0.1:8080",
		"http://127.0.0.1:8080",
		"http://127.8.8.1",
		"https://127.8.8.1",
	}
	path1 := "/vmturbo/remotemediation"

	user := "vmturbo"
	pass := "vmturbo"

	for i := range hosts {
		c, err := NewConnectionConfig(hosts[i], path1, user, pass)
		if err != nil {
			t.Errorf("Failed to create connection config: %v", err)
		}
		fmt.Printf("c[%d] = %++v\n", i, c)
	}
}

// test the readPump()
func TestWSconnection_StartReadWrite(t *testing.T) {
	ch := make(chan struct{}, 1)

	dat := struct{}{}
	ch <- dat

	//1. channel is blocked, and is cleared
	timer := time.NewTimer(time.Second * 10)
	dat2 := struct{}{}
	select {
	case ch <- dat2:
		fmt.Println("data2 is deliverd")
		t.Errorf("channel should be blocked")
	case <-timer.C:
		fmt.Println("channel is blocked, discarding the blocked messages")
		<-ch
	}

	//2. channel is empty
	timer = time.NewTimer(time.Second * 5)
	select {
	case ch <- dat2:
		fmt.Println("push data into channel")
	case <-timer.C:
		fmt.Println("channel is blocked")
		t.Errorf("channel should be empty")
	}
}
