package net

import (
	"log"
	"net"
)

func NewConn(connstr string) net.Conn {
	conn, err := net.Dial("tcp", connstr)
	if err != nil {
		log.Print(err)
		panic(1)
	}
	return conn
}
