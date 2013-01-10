// Package graphite implements sending metrics to Graphite via the Carbon text API
//
// This can be done by sending metrics one at a time or by buffering them through a channel.
package graphite

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"
)

// Graphite is the main interface to the library
type Graphite interface {
	SendMetric()
	Sendall()
	chanRecv()
	chanSend()
	sendMetric()
}

// GraphiteServer is used to store the Graphite parameters
type GraphiteServer struct {
	Host    string
	Port    uint16
	Timeout time.Duration
	conn    net.Conn
}

// Metric contains fields used for sending metrics to Graphite. If Timestamp is not set,
// one is generated when sent.
type Metric struct {
	Name      string
	Value     string
	Timestamp int64
}

// defaultTimeout is the default connection timeout used by DialTimeout.
const (
	defaultTimeout  = 30
	initialWaitTime = 30
)

// Connect is a factory function for Graphite connection
func Connect(graphite GraphiteServer) GraphiteServer {
	return graphite.getconn()
}

// SendMetric is used to send a single metric to Graphite.
// Sets metric.Timestamp to current Unix time if necessary.
func (g *GraphiteServer) SendMetric(metric Metric) {
	if metric.Timestamp == 0 {
		metric.Timestamp = time.Now().Unix()
	}

	g.sendMetric(metric)
}

// Sendall is used to buffered metrics to Graphite via a channel and go routines.
func (g *GraphiteServer) Sendall(buf []Metric) {
	ch := make(chan Metric, len(buf))
	done := make(chan bool)
	go g.chanSend(ch, buf)
	go g.chanRecv(ch, done)

}

// chanRecvMetrics reads `bufsz` numbered metrics off of the given channel and
// sends them to Graphite.
// If a panic() is received within the stack, log the error and stop
// receiving metrics on the channel (preferably to can try and recover).
func (g *GraphiteServer) chanRecv(rch chan Metric, done chan bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
			done <- true
			return
		}
	}()

	for item := range rch {
		g.sendMetric(item)
	}

	done <- true
}

// chanSendMetrics sends a Metric slice to the given channel
func (g *GraphiteServer) chanSend(ch chan Metric, buffer []Metric) {
	for _, item := range buffer {
		if len(item.Name) > 0 {
			log.Printf("buffering %s", item.Name)
			ch <- item
		}
	}
	close(ch)
}

// getconn is the unexported function used to connect to Graphite.
// If there is a problem with the connection it retries with an incremental
// sleep time.
// Returns a pointer to the GraphiteServer struct.
func (g *GraphiteServer) getconn() GraphiteServer {
	var (
		err      error
		waitTime time.Duration
	)

	connectAddr := fmt.Sprintf("%s:%d", g.Host, g.Port)

	if g.Timeout == 0 {
		g.Timeout = defaultTimeout * time.Second
	}

	waitTime = initialWaitTime
	g.conn, err = net.DialTimeout("tcp", connectAddr, g.Timeout)
	for err != nil {
		log.Printf(err.Error())
		log.Printf(fmt.Sprintf("error connecting, retrying in %d seconds", waitTime))
		time.Sleep(waitTime * time.Second)

		waitTime += 5
		g.conn, err = net.DialTimeout("tcp", connectAddr, g.Timeout)
	}

	return *g
}

// sendMetric is the unexported function to send a single metric to Graphite.
// If there is a problem, call panic(). If the problem occurs during a Write() call,
// try to reconnect to Graphite.
func (g *GraphiteServer) sendMetric(metric Metric) error {
	if g.conn == nil {
		panic("no graphite connection?")
	}

	if metric.HasEmptyField() {
		panic("badly formed metric")
	}

	buf := bytes.NewBufferString(fmt.Sprintf("%s %s %d\n", metric.Name, metric.Value, metric.Timestamp))

	log.Printf("sending %s", metric.Name)
	_, err := g.conn.Write(buf.Bytes())
	for err != nil {
		log.Printf("trouble writing metric; retrying: %s", err)
		g.getconn()
		err = g.sendMetric(metric)
	}

	return nil
}

// HasEmptyField is a helper function to determine if any Metric fields are empty.
func (m *Metric) HasEmptyField() bool {
	if len(m.Name) == 0 {
		return true
	} else if len(m.Value) == 0 {
		return true
	}

	return false
}
