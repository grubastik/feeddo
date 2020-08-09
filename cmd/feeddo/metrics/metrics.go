package metrics

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	//MetricTypeFeed defines type for feed metric
	MetricTypeFeed = "feed"
	//MetricTypeTotal defines type for total metric
	MetricTypeTotal = "total"
	//MetricTypeFailed defines type for failed metric
	MetricTypeFailed = "failed"
	//MetricTypeSucceeded defines type for succeeded metric
	MetricTypeSucceeded = "succeeded"
)

// Adder add value from param to internal value
// Gauge and Counter both support method Add
// the only difference is that val could not be negative for Counter
// those metrics are thread safe and use package atomic
// no need to add another atomic operations
type Adder interface {
	Add(float64)
}

// Container holds all metrics
type Container map[string]map[string]Adder

// NewMetrics creates container with all metrics per feed
func NewMetrics(listURL []*url.URL) Container {
	container := make(Container)
	for _, u := range listURL {
		key := u.String()
		if _, ok := container[key]; !ok {
			container[key] = make(map[string]Adder)
		}
		container[key][MetricTypeFeed] = promauto.NewGauge(prometheus.GaugeOpts{
			Name: "feed_processing_" + strings.ReplaceAll(u.Host, ".", "_"),
			Help: "1 indicates that feed start to process and 0 indicates that feed processing ends for url: " + key,
		})
		container[key][MetricTypeTotal] = promauto.NewCounter(prometheus.CounterOpts{
			Name: "total_processed_" + strings.ReplaceAll(u.Host, ".", "_"),
			Help: "Number of items processed for url: " + key,
		})
		container[key][MetricTypeSucceeded] = promauto.NewCounter(prometheus.CounterOpts{
			Name: "succeeded_" + strings.ReplaceAll(u.Host, ".", "_"),
			Help: "Number of items succeeded for url: " + u.String(),
		})
		container[key][MetricTypeFailed] = promauto.NewCounter(prometheus.CounterOpts{
			Name: "failed_" + strings.ReplaceAll(u.Host, ".", "_"),
			Help: "Number of items failed for url: " + u.String(),
		})
	}
	return container
}

// GetMetric returns metric configured. If metric could not be found returns error.
func (c Container) GetMetric(key, typeMetric string) (Adder, error) {
	if v, ok := c[key]; ok {
		if vv, ok := v[typeMetric]; ok {
			return vv, nil
		}
		return nil, fmt.Errorf("Metric of type '%s' is no supported", typeMetric)
	}
	return nil, fmt.Errorf("Metric for key '%s' is not configured", key)
}

// IncrementMetric increments metric
func (c Container) IncrementMetric(key, metricType string) error {
	m, err := c.GetMetric(key, metricType)
	if err != nil {
		return fmt.Errorf("Failed to get metric: %w", err)
	}
	m.Add(1)
	return nil
}
