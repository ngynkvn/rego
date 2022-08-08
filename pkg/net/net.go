package net

import (
	"net"

	"github.com/rs/zerolog/log"
)

func NewConn(connstr string) net.Conn {
	conn, err := net.Dial("tcp", connstr)
	if err != nil {
		log.Fatal().Msgf("[net] unable to connect: %v", err)
	}
	return conn
}
