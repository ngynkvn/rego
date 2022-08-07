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
	Rd  *textproto.Reader
	Out chan RedisMessage
}

// Reset the parsing state of the reader
func (rr *RedisReader) Reset() {

}

func NewRespReader(tp *textproto.Reader, out chan RedisMessage) RedisReader {
	return RedisReader{
		Rd:  tp,
		Out: out,
	}
}

func (rr *RedisReader) Scan() {
	byteCode := rr.TryReadByte()
	switch RespT(byteCode) {
	case BulkString:
		rr.Out <- RespBulkStr(rr)
	case SimpleString:
		rr.Out <- RespSimpleStr(rr)
	case Integer:
		rr.Out <- RespInt(rr)
	case Error:
		rr.Out <- RespError(rr)
	case Arrays:
		// rr.Out <- RespSimpleStr2()
	default:
		rr.Out <- RespSimpleStr(rr)

	}
}

func RespSimpleStr(rr *RedisReader) RedisMessage {
	s, e := rr.Rd.ReadLine()
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal RespSimpleStr: %+v", e))
	}
	return RedisMessage{
		RedisType: RespT(SimpleString),
		Raw:       s,
		Choice:    MsgSimpleStr(s),
	}
}

func RespError(rr *RedisReader) RedisMessage {
	s, e := rr.Rd.ReadLine()
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal RespError: %+v", e))
	}
	return RedisMessage{
		RedisType: RespT(Error),
		Raw:       s,
		Choice:    MsgError(s),
	}
}

func RespBulkStr(rr *RedisReader) RedisMessage {
	len := 0
	for b := rr.TryReadByte(); b != '\r'; b = rr.TryReadByte() {
		len = (len * 10) + int(b-byte('0'))
	}
	bulkStr := make([]byte, len)
	io.ReadFull(rr.Rd.R, bulkStr)
	_, e := rr.Rd.R.Discard(2)
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal: %+v", e))
	}
	return RedisMessage{
		RedisType: BulkString,
		Raw:       string(bulkStr),
		Choice:    MsgBulkStr(string(bulkStr)),
	}
}

func RespInt(rr *RedisReader) RedisMessage {
	s, e := rr.Rd.ReadLine()
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal RespInt: %+v", e))
	}

	i, e := strconv.ParseInt(s, 10, 64)
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal RespInt: %+v", e))
	}

	return RedisMessage{
		RedisType: Integer,
		Raw:       fmt.Sprintf("%d", i),
		Choice:    MsgInteger(i),
	}
}

func (rr *RedisReader) TryReadByte() byte {
	b, e := rr.Rd.R.ReadByte()
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal TryReadByte: %+v", e))
	}
	return b
}
