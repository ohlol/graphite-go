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

// Graphite is the struct by which we interface with the library. At minimum you must set
// Host and Port fields.
type Graphite struct {
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

var doneSending bool

// connect is the unexported function used for connecting
func connect(host string, port uint16, timeout time.Duration) (net.Conn, error) {
	connectAddr := fmt.Sprintf("%s:%d", host, port)

	return net.DialTimeout("tcp", connectAddr, timeout)
}

// sendMetric is the unexported function to send a single metric to Graphite.
func sendMetric(conn net.Conn, metric Metric) {
	log.Printf("sending %s", metric.Name)
	fmt.Fprintf(conn, "%s %s %s", metric.Name, metric.Value, metric.Timestamp)
}

// chanSendMetrics sends a Metric slice to the given channel
func chanSendMetrics(ch chan Metric, buffer []Metric) {
	for _, item := range buffer {
		if len(item.Name) > 0 {
			log.Printf("buffering %s", item.Name)
			ch <- item
		}
	}
}

// chanRecvMetrics reads `bufsz` numbered metrics off of the given channel and
// sends them to Graphite.
func chanRecvMetrics(ch chan Metric, conn net.Conn, bufsz int) {
	for i := 0; i < bufsz; i++ {
		item := <-ch
		sendMetric(conn, item)
	}

	doneSending = true
}

// Connect wraps the unexported connect function.
func (g *Graphite) Connect() {
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
func (g *Graphite) SendMetric(metric Metric) {
	if metric.Timestamp == 0 {
		metric.Timestamp = time.Now().Unix()
	}

	sendMetric(g.conn, metric)
}

// Sendall is used to buffered metrics to Graphite via a channel and go routines.
func (g *Graphite) Sendall(buf []Metric) {
	doneSending = false

	ch := make(chan Metric, len(buf))
	go chanSendMetrics(ch, buf)
	go chanRecvMetrics(ch, g.conn, len(buf))

	for doneSending == false {
		time.Sleep(1 * time.Second)
	}
}
