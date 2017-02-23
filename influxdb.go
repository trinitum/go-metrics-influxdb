package influxdb

import (
	"log"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
)

type Config struct {
	Registry       metrics.Registry
	FlushInterval  time.Duration
	InfluxDBClient client.Client
	Measurement    string
	Tags           map[string]string
}

func InfluxDB(c *Config) {
	c.run()
}

func (c *Config) run() {
	c.reportMetrics()
	if c.FlushInterval > 0 {
		for _ = range time.Tick(c.FlushInterval) {
			c.reportMetrics()
		}
	}
}

func (c *Config) reportMetrics() {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{Precision: "s"})
	if err != nil {
		log.Printf("Couldn't create a new batch points object: %v\n", err)
	}
	now := time.Now()
	c.Registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			c.addPoint(bp, name, metric.Count(), now)
		case metrics.Gauge:
			c.addPoint(bp, name, metric.Value(), now)
		case metrics.GaugeFloat64:
			c.addPoint(bp, name, metric.Value(), now)
		default:
			log.Printf("don't know how to process metric %v of type %T\n", name, i)
		}
	})
	c.InfluxDBClient.Write(bp)
}

func (c *Config) addPoint(bp client.BatchPoints, name string, value interface{}, now time.Time) {
	pt, err := client.NewPoint(c.Measurement, c.Tags, map[string]interface{}{name: value}, now)
	if err != nil {
		log.Printf("Couldn't create a point {%v, %v}: %v", name, value, err)
		return
	}
	bp.AddPoint(pt)
}
