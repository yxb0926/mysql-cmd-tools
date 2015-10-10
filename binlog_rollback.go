package main

import (
	//"bufio"
	//"io"
	"io/ioutil"
	//"reflect"
	"bytes"
	"encoding/binary"
	"fmt"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

var magicnum [4]byte

type EventType byte

const (
	UNKNOWN_EVENT EventType = iota
	START_EVENT_V3
	QUERY_EVENT
	STOP_EVENT
	ROTATE_EVENT
	INTVAR_EVENT
	LOAD_EVENT
	SLAVE_EVENT
	CREATE_FILE_EVENT
	APPEND_BLOCK_EVENT
	EXEC_LOAD_EVENT
	DELETE_FILE_EVENT
	NEW_LOAD_EVENT
	RAND_EVENT
	USER_VAR_EVENT
	FORMAT_DESCRIPTION_EVENT
	XID_EVENT
	BEGIN_LOAD_QUERY_EVENT
	EXECUTE_LOAD_QUERY_EVENT
	TABLE_MAP_EVENT
	WRITE_ROWS_EVENTv0
	UPDATE_ROWS_EVENTv0
	DELETE_ROWS_EVENTv0
	WRITE_ROWS_EVENTv1
	UPDATE_ROWS_EVENTv1
	DELETE_ROWS_EVENTv1
	INCIDENT_EVENT
	HEARTBEAT_EVENT
	IGNORABLE_EVENT
	ROWS_QUERY_EVENT
	WRITE_ROWS_EVENTv2
	UPDATE_ROWS_EVENTv2
	DELETE_ROWS_EVENTv2
	GTID_EVENT
	ANONYMOUS_GTID_EVENT
	PREVIOUS_GTIDS_EVENT
)

type EventHeader struct {
	Timestamp uint32
	EventType byte
	ServerID  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

type FormatDescriptionEvent struct {
	header            EventHeader
	BinlogVersion     uint16
	ServerVersion     string
	CreateTime        uint32
	HeaderLength      uint8
	PostHeaderLength  []byte
	ChecksumAlgorithm byte
}

func main() {
	buff, err := ioutil.ReadFile("./mysql-bin.000045")
	check(err)
	buf := bytes.NewBuffer(buff)

	var magicid [4]byte
	//var timestamp uint32
	//var eventType uint8
	//var serverId uint32
	event := new(FormatDescriptionEvent)
	err = binary.Read(buf, binary.LittleEndian, &magicid)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.BinlogVersion)
	event.ServerVersion = string(buf.Next(50))
	err = binary.Read(buf, binary.LittleEndian, &event.CreateTime)
	err = binary.Read(buf, binary.LittleEndian, &event.HeaderLength)
	err = binary.Read(buf, binary.LittleEndian, &event.PostHeaderLength)
	//err = binary.Read(buf, binary.LittleEndian, &magicid)
	//err = binary.Read(buf, binary.LittleEndian, &timestamp)
	//err = binary.Read(buf, binary.LittleEndian, &eventType)
	//err = binary.Read(buf, binary.LittleEndian, &serverId)
	check(err)
	fmt.Println("FDE Header - Timestamp:  ", event.header.Timestamp)
	fmt.Println("FDE Header - EventType:  ", event.header.EventType)
	fmt.Println("FDE Header - ServerID:  ", event.header.ServerID)
	fmt.Println("FDE Header - EventSize:  ", event.header.EventSize)
	fmt.Println("FDE Header - LogPos:  ", event.header.LogPos)
	fmt.Println("FDE Header - Flags:  ", event.header.Flags)
	fmt.Println("FDE Header - BinlogVersion:  ", event.BinlogVersion)
	fmt.Println("FDE Header - ServerVersion:  ", event.ServerVersion)
	fmt.Println("FDE Header - CreateTime:  ", event.CreateTime)
	fmt.Println("FDE Header - HeaderLength:  ", event.HeaderLength)
	fmt.Println("FDE Header - HeaderLength:  ", event.PostHeaderLength)

}
