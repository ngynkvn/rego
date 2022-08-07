package benchmark

import (
	"bufio"
	"bytes"
	"net/textproto"
	"testing"

	"github.com/ngynkvn/rego/pkg/resp"
)

type TestCase[T, K any] struct {
	Given    T
	Expected K
}

const tc_simple_bulk_str = "$5\r\n" + "hello\r\n"

func initReader(s string) (resp.RedisReader, chan resp.RedisMessage) {
	ch := make(chan resp.RedisMessage)
	return resp.NewRespReader(textproto.NewReader(bufio.NewReader(bytes.NewBufferString(s))), ch), ch
}
func BenchmarkScan(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		rr, ch := initReader(tc_simple_bulk_str)
		go func() {
			<-ch
		}()
		b.StartTimer()
		rr.Scan()
	}
}
