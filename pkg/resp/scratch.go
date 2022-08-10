package resp

import (
	"fmt"
	"strconv"

	"github.com/rs/zerolog/log"
)

// Scratch file for testing out programming ideas
// Ideas may be silly. View at your own risk of going ???

// _Pipe is intended to allow chaining methods in a pipeline like fashion.
// Crashes on first error encounter.
//
// It also doesn't really work. Ideally we wouldn't have to specify the types at all
// We just want to guarantee the types match properly between functions passed but
// I'm not sure if we can with Go's version of generics
type _Pipe[T, K any] func() (T, error)
type _Then[T, K any] interface {
	_Then(func(T) (K, error)) _Pipe[T, K]
}

func (p _Pipe[T, K]) _Then(fn func(T) (K, error)) _Pipe[K, any] {
	r, err := p()
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal pipe: %+v", err))
	}
	return _Pipe[K, any](func() (K, error) {
		return fn(r)
	})
}

func _RespInt(rr *RedisReader) RedisMessage {
	i, err := _Pipe[string, int64](rr.Rd.ReadLine)._Then(func(s string) (int64, error) {
		return strconv.ParseInt(s, 10, 64)
	})()

	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal RespInt: %+v", err))
	}
	return RedisMessage{
		RedisType: Integer,
		Raw:       fmt.Sprintf("%d", i),
		Choice:    MsgInteger(i),
	}
}
