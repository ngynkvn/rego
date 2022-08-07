package main

import (
	"bufio"
	"fmt"
	"net/textproto"
	"os"
	"strings"
	"sync"

	"github.com/ngynkvn/rego/pkg/net"
	"github.com/ngynkvn/rego/pkg/resp"
)

// loopScan loops on redis connection and reads the incoming lines
//
// Sends out messages through channel
func loopScan(reader *textproto.Reader, msg_ch chan resp.RedisMessage) {
	rr := resp.NewRespReader(reader, msg_ch)
	for {
		rr.Scan()
	}
}

func repl(input *textproto.Reader, wr *textproto.Writer) {
	for {
		print("> ")
		input_str, err := input.ReadLine()
		if len(input_str) > 0 && err == nil {
			// The redis server expects arrays of bulk strings for sending commands.
			commands := strings.Split(input_str, " ")
			// Specify the array length and create the bulk strings
			pipe := []string{fmt.Sprintf("*%d", len(commands))}
			pipe = append(pipe, createBulkStrings(commands)...)
			for _, out := range pipe {
				wr.PrintfLine(out)
			}
		}
	}
}

// createBulkStrings converts normal strings into bulk strings to send to redis.
func createBulkStrings(commands []string) []string {
	cmds := make([]string, len(commands)*2)
	for i, v := range commands {
		cmds[i*2] = fmt.Sprintf("$%d", len(commands[i]))
		cmds[(i*2)+1] = v
	}
	return cmds
}

// cmd/client starts a simple Read-Send-Print-Loop (RSPL?)
// with a redis server.
//
// TODO: Implement command flags
func main() {
	// Create the connection to the redis server
	// TODO: cli, rm hardcode
	conn := net.NewConn("127.0.0.1:6379")
	defer conn.Close()

	// Create the readers and writers we will pass to our subroutines
	msg_channel := make(chan resp.RedisMessage)
	write_sock := textproto.NewWriter(bufio.NewWriter(conn))
	read_sock := textproto.NewReader(bufio.NewReader(conn))
	read_tty := textproto.NewReader(bufio.NewReader(os.Stdin))
	write_tty := textproto.NewWriter(bufio.NewWriter(os.Stdout))

	runAsWaitGroup(
		// Subscribe to redis connection
		func() {
			loopScan(read_sock, msg_channel)
		},
		// Read user input
		func() {
			repl(read_tty, write_sock)
		},
		// Write to output
		func() {
			for {
				msg := <-msg_channel
				write_tty.PrintfLine("[%c] %s", msg.RedisType, msg.Choice.String())
			}
		},
	).Wait()

}

// runAsWaitGroup runs closures within a sync.WaitGroup
//
// This function blocks until all closures finish running.
func runAsWaitGroup(closures ...func()) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	for _, fn := range closures {
		wg.Add(1)
		go func(fn func()) {
			fn()
			wg.Done()
		}(fn)
	}
	return &wg
}
