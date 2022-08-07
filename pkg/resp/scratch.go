package resp

import (
	"fmt"

	"github.com/rs/zerolog/log"
)

// Scratch file for testing out programming ideas

// Pipe is intended to allow chaining methods in a pipeline like fashion.
// Crashes on first error encounter.
type Pipe[T, K any] func() (T, error)
type Then[T, K any] interface {
	Then(func(T) (K, error)) Pipe[T, K]
}

func (p Pipe[T, K]) Then(fn func(T) (K, error)) Pipe[K, any] {
	r, err := p()
	if err != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal pipe: %+v", err))
	}
	return Pipe[K, any](func() (K, error) {
		return fn(r)
	})
}

// func RespInt2(rr *RedisReader) RedisMessage {
// 	i, err := Pipe[string, int64](rr.Rd.ReadLine).Then(func(s string) (int64, error) {
// 		return strconv.ParseInt(s, 10, 64)
// 	})()

// 	if err != nil {
// 		log.Fatal().Msg(fmt.Sprintf("Fatal RespInt: %+v", err))
// 	}
// 	return RedisMessage{
// 		RedisType: Integer,
// 		Raw:       fmt.Sprintf("%d", i),
// 		Choice:    MsgInteger(i),
// 	}
// }
