package resp

// My intepretation of the RESP protocol.
// Referenced from https://redis.io/docs/reference/protocol-spec/
// 8-7-2022

import (
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"strings"

	"github.com/rs/zerolog/log"
)

type RespT byte

const (
	SimpleString RespT = '+'
	Error        RespT = '-'
	Integer      RespT = ':'
	BulkString   RespT = '$'
	Array        RespT = '*'
)

type Message interface {
	String() string
}

type MsgSimpleStr string
type MsgError string
type MsgBulkStr string
type MsgInteger int64
type MsgArray []Message

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

func (m MsgArray) String() string {
	return fmt.Sprintf("[%s]", strings.Join(Map(m, Message.String), ","))
}

func (m RedisMessage) String() string {
	return m.Choice.String()
}

type RedisMessage struct {
	RedisType RespT
	Raw       string
	Choice    Message
}

func (m RedisMessage) Symbol() rune {
	return rune(m.RedisType)
}

func (msg RedisMessage) AsString() (string, error) {
	if conv, ok := msg.Choice.(MsgSimpleStr); ok {
		return string(conv), nil
	} else if conv, ok := msg.Choice.(MsgBulkStr); ok {
		return string(conv), nil
	} else {
		return "", NewParseError(msg.RedisType, SimpleString, BulkString)
	}
}

func (msg RedisMessage) AsInteger() (int64, error) {
	if conv, ok := msg.Choice.(MsgInteger); ok {
		return int64(conv), nil
	} else {
		return 0, NewParseError(msg.RedisType, Integer)
	}
}

func (msg RedisMessage) AsError() (error, error) {
	if conv, ok := msg.Choice.(MsgError); ok {
		return errors.New(string(conv)), nil
	} else {
		return nil, NewParseError(msg.RedisType, Error)
	}
}

func (msg RedisMessage) AsArray() ([]Message, error) {
	if conv, ok := (msg.Choice).(MsgArray); ok {
		return conv, nil
	} else {
		return nil, NewParseError(msg.RedisType, Array)
	}
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

// Fetch grabs the next incoming RESP type from the IO input
func Fetch(rr *RedisReader) RedisMessage {
	byteCode := rr.TryReadByte()
	switch RespT(byteCode) {
	case BulkString:
		return RespBulkStr(rr)
	case SimpleString:
		return RespSimpleStr(rr)
	case Integer:
		return RespInt(rr)
	case Error:
		return RespError(rr)
	case Array:
		return RespArray(rr)
	default:
		panic(fmt.Sprint("Unknown bytecode: ", byteCode))
	}
}

// Scan is the main API to send redis messages out to a channel.
func (rr *RedisReader) Scan() {
	rr.Out <- Fetch(rr)
}

// RespSimpleStr attempts to parse a string from the resulting line.
//
// TODO: I don't think we need the high level facilities from net/textproto anymore
// 	     since the parsing is quite trivial.
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

// RespSimpleStr attempts to parse a string from the resulting line.
//
// It first expects a length value parsed as an integer,
// then directly copies the subsequent payload into a string
// buffer
func RespBulkStr(rr *RedisReader) RedisMessage {
	len := loopReadInt(rr)

	// Read the string data into buffer
	bulkStr := make([]byte, len)
	io.ReadFull(rr.Rd.R, bulkStr)

	// Discard two bytes since we don't need the '\r\n'
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

// RespInt reads an integer from the IO stream
func RespInt(rr *RedisReader) RedisMessage {
	val := loopReadInt(rr)
	return RedisMessage{
		RedisType: Integer,
		Raw:       fmt.Sprintf("%d", val),
		Choice:    MsgInteger(val),
	}
}

// RespArray reads an array of RESP objects from the IO stream
func RespArray(rr *RedisReader) RedisMessage {
	len := loopReadInt(rr)
	fetched := make([]RedisMessage, 0, len)
	for i := 0; i < len; i++ {
		fetched[i] = Fetch(rr)
	}
	return RedisMessage{
		RedisType: Array,
		Raw:       fmt.Sprint(Map(fetched, func(rm RedisMessage) string { return rm.Raw })),
		Choice:    MsgArray(Map(fetched, func(rm RedisMessage) Message { return rm.Choice })),
	}
}

// loopReadInt reads from IO and returns an integer, discarding \r\n.
func loopReadInt(rr *RedisReader) int {
	val := 0
	for b := rr.TryReadByte(); b != '\r'; b = rr.TryReadByte() {
		val = (val * 10) + int(b-byte('0'))
	}
	_, e := rr.Rd.R.Discard(1)
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal loopReadInt: %+v", e))
	}
	return val
}

// TryReadByte attempts to read a single byte from IO and panics on any error
func (rr *RedisReader) TryReadByte() byte {
	b, e := rr.Rd.R.ReadByte()
	if e != nil {
		log.Fatal().Msg(fmt.Sprintf("Fatal TryReadByte: %+v", e))
	}
	return b
}

// Map. Your standard good old fashioned generic map function :)
//
// A la []T -> []K
func Map[T any, K any](slice []T, fn func(T) K) []K {
	out := make([]K, 0, len(slice))
	for i := range out {
		out[i] = fn(slice[i])
	}
	return out
}
