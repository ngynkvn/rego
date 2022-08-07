package resp

// My intepretation of the RESP protocol.
// Referenced from https://redis.io/docs/reference/protocol-spec/
// 8-7-2022

import (
	"fmt"
	"io"
	"net/textproto"
	"strconv"

	"github.com/rs/zerolog/log"
)

type RespT byte

const (
	SimpleString RespT = '+'
	Error        RespT = '-'
	Integer      RespT = ':'
	BulkString   RespT = '$'
	Arrays       RespT = '*'
)

type MsgSimpleStr string
type MsgError string
type MsgBulkStr string
type MsgInteger int64

func (m MsgInteger) String() string {
	return fmt.Sprintf("%d", m)
}

func (m MsgBulkStr) String() string {
	return string(m)
}

func (m MsgError) String() string {
	return string(m)
}

func (m MsgSimpleStr) String() string {
	return string(m)
}

type Message interface {
	String() string
}

type RedisMessage struct {
	RedisType RespT
	Raw       string
	Choice    Message
}

func (m RedisMessage) Symbol() rune {
	return rune(m.RedisType)
}

func MakeParser[T any](t RespT, conv func(string) (T, error)) func(msg RedisMessage) (T, error) {
	return func(msg RedisMessage) (T, error) {
		if msg.RedisType == t {
			result, e := conv(msg.Raw[1:])
			if e != nil {
				return result, NewParseError(msg.RedisType, t)
			}
			return result, nil
		} else {
			return *new(T), NewParseError(msg.RedisType, t)
		}
	}
}

var (
	msg_str = MakeParser(SimpleString, func(s string) (string, error) {
		return s, nil
	})
	msg_int = MakeParser(Integer, func(s string) (int64, error) {
		return strconv.ParseInt(s, 10, 64)
	})
)

func (msg RedisMessage) String() (string, error) {
	return msg_str(msg)
}

func (msg RedisMessage) Integer() (int64, error) {
	return msg_int(msg)
}

type RedisReader struct {
	rd  *textproto.Reader
	out chan RedisMessage
}

// Reset the parsing state of the reader
func (rr *RedisReader) Reset() {

}

func NewRespReader(tp *textproto.Reader, out chan RedisMessage) RedisReader {
	return RedisReader{
		rd:  tp,
		out: out,
	}
}

func (rr *RedisReader) Scan() {
	str, e := rr.rd.ReadLine()
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal: %+v", e))
		rr.Reset()
		return
	}
	switch RespT(str[0]) {
	case BulkString:
		rr.out <- respBulkStr(str, rr)
	case SimpleString:
		rr.out <- respSimpleStr(str)
	case Integer:
		rr.out <- respInt(str)
	case Error:
		rr.out <- respError(str)
	default:
		rr.out <- respSimpleStr(str)
	}
}

func respSimpleStr(str string) RedisMessage {
	return RedisMessage{
		RedisType: RespT(str[0]),
		Raw:       str,
		Choice:    MsgSimpleStr(str[1:]),
	}
}

func respError(str string) RedisMessage {
	return RedisMessage{
		RedisType: RespT(str[0]),
		Raw:       str,
		Choice:    MsgError(str[1:]),
	}
}

func respBulkStr(str string, rr *RedisReader) RedisMessage {
	l, e := strconv.ParseInt(str[1:], 10, 64)
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal: %+v", e))
	}
	if l == -1 {
		return RedisMessage{
			RedisType: RespT(str[0]),
			Raw:       str,
			Choice:    MsgBulkStr("<nil>"),
		}
	}
	bulkStr := make([]byte, l)
	io.ReadFull(rr.rd.R, bulkStr)
	_, e = rr.rd.R.Discard(2)
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal: %+v", e))
	}
	return RedisMessage{
		RedisType: RespT(str[0]),
		Raw:       str + "\n" + string(bulkStr),
		Choice:    MsgBulkStr(string(bulkStr)),
	}
}

func respInt(str string) RedisMessage {
	i, e := strconv.ParseInt(str[1:], 10, 64)
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal: %+v", e))
	}
	return RedisMessage{
		RedisType: RespT(str[0]),
		Raw:       str,
		Choice:    MsgInteger(i),
	}
}
