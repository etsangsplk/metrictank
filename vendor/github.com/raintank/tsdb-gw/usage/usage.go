package usage

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
)

type EventType byte

var (
	DataPointReceived EventType = 'd'
	ApiRequest        EventType = 'r'
)

type Event struct {
	ID    string
	Org   int32
	EType EventType
}

type TsdbUsage struct {
	buf      []string
	In       chan string
	out      chan output
	conn     net.Conn
	addr     *net.TCPAddr
	shutdown chan struct{}
	wg       sync.WaitGroup
}

type output struct {
	buf []string
	ts  time.Time
}

var usage *TsdbUsage

func Init(addr string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	usage = &TsdbUsage{
		buf:      make([]string, 0, 1000),
		In:       make(chan string, 1000),
		out:      make(chan output, 10),
		addr:     tcpAddr,
		shutdown: make(chan struct{}),
	}
	usage.wg.Add(2)
	go usage.process()
	go usage.flush()
	return nil
}

func Stop() {
	usage.Stop()
}

func (u *TsdbUsage) Stop() {
	close(u.shutdown)
	u.wg.Wait()
}

func LogDataPoint(id string) {
	if usage != nil {
		usage.In <- fmt.Sprintf("%c%s", DataPointReceived, id)
	}
}

func LogRequest(org int, request string) {
	if usage != nil {
		usage.In <- fmt.Sprintf("%c%d.%s", ApiRequest, org, request)
	}
}

func (t *TsdbUsage) process() {
	defer t.wg.Done()
	ticker := time.NewTicker(time.Millisecond * time.Duration(50))
	for {
		select {
		case id := <-t.In:
			t.buf = append(t.buf, id)
		case ts := <-ticker.C:
			// non-blocking write to output buffer.
			// if we succesfully add the current input buffer
			// to the output chan then we re-initialize the input
			// buffer to an new empty slice.  If the output chan is
			// blocked, then we just move on and will retry next flush.
			select {
			case t.out <- output{buf: t.buf, ts: ts}:
				t.buf = make([]string, 0, 1000)
			default:
				log.Warn("Usage: output buffer full.")
			}
		case <-t.shutdown:
			select {
			case t.out <- output{buf: t.buf, ts: time.Now()}:
				t.buf = make([]string, 0, 1000)
			default:
				log.Warn("Usage: output buffer full.")
			}
			close(t.out)
			return
		}
	}
}

func (t *TsdbUsage) flush() {
	defer t.wg.Done()
LOOP:
	for output := range t.out {
		// if our payload is more then 10seconds old, then just drop it.
		if time.Since(output.ts) > time.Second*time.Duration(10) {
			continue
		}
		// dont spend more then 10seconds trying to send data.
		deadline := time.Now().Add(time.Second * time.Duration(10))

		var err error
		if t.conn == nil {
			connected := t.reconnect(deadline)
			if !connected {
				// could not connect after 10seconds.  dropping data.
				continue
			}
		}
		t.conn.SetWriteDeadline(deadline)
		for _, data := range output.buf {
			for {
				_, err = t.conn.Write(append([]byte(data), '\n'))
				if err != nil {
					t.conn.Close()
					t.conn = nil
					log.Error(3, "error while writting to connection. %v", err)
					if deadline.Sub(time.Now()) < time.Duration(0) {
						log.Error(3, "Usage: failed to write data before deadline.")
						continue LOOP
					}
					connected := t.reconnect(deadline)
					if !connected {
						// could not connect after 10seconds.  dropping data.
						continue LOOP
					}
				} else {
					break
				}
			}

		}
	}
}

func (t *TsdbUsage) reconnect(deadline time.Time) bool {
	connected := false
	var err error
	for deadline.Sub(time.Now()) > time.Duration(0) {
		t.conn, err = net.DialTCP("tcp", nil, t.addr)
		if err == nil {
			connected = true
			break
		}
		t.conn = nil
		log.Error(3, "failed to connect to tsdb-usage server. %v", err)
		time.Sleep(time.Second)
	}
	return connected
}
