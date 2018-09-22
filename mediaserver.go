package gortmp

import (
	"fmt"
	"lino-network/goflv_z"
	"lino-network/golog_z"
	"net"
	"os"
	"sync"
)

type MasterConn struct {
	in_conn   InboundConn
	in_stream InboundStream
}

type MediaServer struct {
	master    *MasterConn
	stopped   bool
	streamsMu sync.Mutex
	server    *Server
}

func NewMediaServer() (*MediaServer, error) {
	ms := &MediaServer{
		master: &MasterConn{},
	}
	return ms, nil
}

func (mc *MasterConn) SetConn(conn InboundConn) {
	mc.in_conn = conn
}

func (mc *MasterConn) SetStream(stream InboundStream) {
	mc.in_stream = stream
}

func (ms *MediaServer) MasterConn() *MasterConn {
	return ms.master
}

func (ms *MediaServer) ResetMasterConn() {
	ms.master = nil
}

func (ms *MediaServer) Serve(listenIp *string, port *string) {
	server, err := NewServer("tcp", net.JoinHostPort(*listenIp, *port), ms)
	if err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_FATAL, "MediaServer::Serve(): NewServer error = ", err)
		os.Exit(-1)
	}
	ms.server = server

	// try 5 times
	var retry int = 5
	for {
		workerAddr, ok := ms.server.listener.Addr().(*net.TCPAddr)
		if !ok {
			ms.server.listener.Close()
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING, "MediaServer::Server(): get non tcp addr: %s", ms.server.listener.Addr())
			retry--
		} else {
			logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "MediaServer::Serve(): listen to = ", net.JoinHostPort(*listenIp, fmt.Sprintf("%d", workerAddr.Port)))
			break
		}

		if retry < 0 {
			break
		}
	}

	if retry < 0 {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_FATAL, "MediaServer::Server(): fail to get tcp addr, terminate now ...")
		ms.master.in_conn.Close()
		ms.ResetMasterConn()
	} else {
		ms.server.Serve()
	}
}

// Server handler functions
func (ms *MediaServer) NewConnection(ibConn InboundConn, connectReq *Command, server *Server) bool {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "MediaServerHandlerImpl::NewConnection(): New Connection")
	ibConn.Attach(ms)
	return true
}

// InboundConn handler funcions
func (ms *MediaServer) OnReceived(conn Conn, message *Message) {

}

func (ms *MediaServer) OnReceivedRtmpCommand(conn Conn, command *Command) {
	// never triggered
}

func (ms *MediaServer) OnClosed(conn Conn) {
	// never triggered, InboundConn.OnClosed() would trigger OnStatus()
}

func (ms *MediaServer) OnStatus(conn InboundConn) {
	// triggered when OnStreamCreated() or OnClosed()
	status, err := conn.Status()
	if err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING, "MediaServer::OnStatus(): err = ", err)
	}
	if conn == ms.master.in_conn {
		if status == INBOUND_CONN_STATUS_CLOSE {
			ms.server.Close()
			ms.ResetMasterConn()
			logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnStatus(): master connection closed")
			return
		}
	}
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnStatus(): status = ", status)
}

func (ms *MediaServer) OnStreamCreated(conn InboundConn, stream InboundStream) {
	if conn == ms.master.in_conn {
		if ms.master.in_stream == nil {
			ms.master.SetStream(stream)
		} else {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING, "MediaServer::OnStreamCreated(): multiple master streams(id=%d) created", stream.ID())
		}
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnStreamCreated(): master stream(id=%d) created", stream.ID())
	} else {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnStreamCreated(): stream(id=%d) created", stream.ID())
	}
	stream.Attach(ms)
}

func (ms *MediaServer) OnStreamClosed(conn InboundConn, stream InboundStream) {
	// never triggered
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnStreamClosed(): should never trigger")
}

// Stream handle functions
func (ms *MediaServer) OnPlayStart(stream InboundStream) {
	if stream == ms.master.in_stream {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_FATAL, "MediaServer::OnPlayStart(): master stream should only do publish, stopping now ...")
		stream.Conn().Close()
		return
	}
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnPlayStart(), stream.StreamName()")

	// init MediaPlayer
}

func (ms *MediaServer) OnPublishStart(stream InboundStream) {
	if stream != ms.master.in_stream {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_FATAL, "MediaServer::OnPublishStart(): servering stream should only do play, terminate current connection now ...")
		stream.Conn().Close()
		return
	}
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnPublishStart() name=", stream.StreamName())

	// init pub/sub here
}

func (ms *MediaServer) OnReceiveAudio(stream InboundStream, on bool) {
	// never trigger
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnReceiveAudio() ON=%b", on)
}

func (ms *MediaServer) OnReceiveVideo(stream InboundStream, on bool) {
	// never trigger
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_INFO, "MediaServer::OnReceiveVideo() ON=%b", on)
}

func (ms *MediaServer) OnFlvTag(stream InboundStream, flvTag *flv.FlvTag) {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_INFO, "msg type = ", flvTag.Type, ", size = ", flvTag.Size, ", bytes = ", len(flvTag.Bytes))
}
