// Package graphite implements sending metrics to Graphite via the Carbon text API
//
// This can be done by sending metrics one at a time or by buffering them through a channel.
package graphite

import (
	"fmt"
	"log"
	"net"
	"time"
)

// Interface type for Graphite.
type Graphite interface {
	Connect()
	SendMetric()
	Sendall()
	chanRecv()
	chanSend()
	sendMetric()
}

// Graphite is how we interface with the library. At minimum you must set
// Host and Port fields.
type GraphiteServer struct {
	Host    string
	Port    uint16
	Timeout time.Duration
	conn    net.Conn
}

// Metric is contains fields used for sending metrics to Graphite. If Timestamp is not set,
// one is generated when sent.
type Metric struct {
	Name      string
	Value     string
	Timestamp int64
}

// defaultTimeout is the default connection timeout used by DialTimeout.
const (
	defaultTimeout = 30
)

var doneSending = false

// connect is the unexported function used for connecting
func connect(host string, port uint16, timeout time.Duration) (net.Conn, error) {
	connectAddr := fmt.Sprintf("%s:%d", host, port)

	return net.DialTimeout("tcp", connectAddr, timeout)
}

// Connect wraps the unexported connect function.
// Sets a connection timeout if unset.
func (g *GraphiteServer) Connect() {
	var (
		err     error
		timeout time.Duration
	)

	if g.Timeout == 0 {
		g.Timeout = defaultTimeout
	}

	timeout = g.Timeout * time.Second

	g.conn, err = connect(g.Host, g.Port, timeout)
	for err != nil {
		log.Printf(err.Error())
		log.Printf(fmt.Sprintf("error connecting, retrying in %d seconds", int(timeout.Seconds())))
		time.Sleep(timeout)

		timeout = (g.Timeout + 5) * time.Second
		g.conn, err = connect(g.Host, g.Port, timeout)
	}
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
	doneSending = false

	ch := make(chan Metric, len(buf))
	go g.chanSend(ch, buf)
	go g.chanRecv(ch, len(buf))

	for doneSending == false {
		time.Sleep(1 * time.Second)
	}
}

// chanRecvMetrics reads `bufsz` numbered metrics off of the given channel and
// sends them to Graphite.
func (g *GraphiteServer) chanRecv(ch chan Metric, bufsz int) {
	for i := 0; i < bufsz; i++ {
		item := <-ch
		g.sendMetric(item)
	}

	doneSending = true
}

// chanSendMetrics sends a Metric slice to the given channel
func (g *GraphiteServer) chanSend(ch chan Metric, buffer []Metric) {
	for _, item := range buffer {
		if len(item.Name) > 0 {
			log.Printf("buffering %s", item.Name)
			ch <- item
		}
	}
}

// sendMetric is the unexported function to send a single metric to Graphite.
func (g *GraphiteServer) sendMetric(metric Metric) {
	log.Printf("sending %s", metric.Name)
	fmt.Fprintf(g.conn, "%s %s %d\n", metric.Name, metric.Value, metric.Timestamp)
}
