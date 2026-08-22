package broker

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"

	"github.com/AfzalRaja001/kafka-go/internal/group"
	"github.com/AfzalRaja001/kafka-go/internal/protocol"
	"github.com/AfzalRaja001/kafka-go/internal/storage"
)

// Serve listens on address and accepts connections until ctx is canceled,
// at which point it closes the listener and returns cleanly.
func Serve(ctx context.Context, address string, registry *protocol.TopicRegistry, brokers []protocol.Broker, diskLog storage.Log, offsets group.OffsetStore) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go handleConn(conn, registry, brokers, diskLog, offsets)
	}
}

func handleConn(conn net.Conn, registry *protocol.TopicRegistry, brokers []protocol.Broker, diskLog storage.Log, offsets group.OffsetStore) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	for {
		msg, err := ReadMessage(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return // client disconnected cleanly between messages
			}
			log.Printf("broker: connection error: %v", err)
			return
		}

		resp, err := dispatch(msg, registry, brokers, diskLog, offsets)
		if err != nil {
			log.Printf("broker: dispatch error: %v", err)
			return
		}

		if err := WriteMessage(writer, resp); err != nil {
			log.Printf("broker: write error: %v", err)
			return
		}
		if err := writer.Flush(); err != nil {
			log.Printf("broker: flush error: %v", err)
			return
		}
	}
}
