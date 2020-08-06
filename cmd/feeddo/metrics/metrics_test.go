package metrics

import (
	"net/url"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	testURL, err := url.Parse("http://test.com")
	require.NoError(t, err)
	urls := []*url.URL{testURL}
	c := NewMetrics(urls)
	require.NotEmpty(t, c)
	require.NotEmpty(t, c[testURL.String()])
	for _, key := range []string{"feed", "total", "succeeded", "failed"} {
		assert.NotEmpty(t, c[testURL.String()][key])
		assert.Implements(t, (*Adder)(nil), c[testURL.String()][key])
	}
}

func TestGetMetric(t *testing.T) {
	m := make(Container)
	m["a"] = make(map[string]Adder)
	m["a"]["b"] = promauto.NewCounter(prometheus.CounterOpts{Name: "test", Help: "test"})
	tests := []struct {
		name       string
		key        string
		metricType string
		err        string
	}{
		{"Key does not exist", "b", "", "Metric for key 'b' is not configured"},
		{"metric type does not exist", "a", "c", "Metric of type 'c' is no supported"},
		{"happy path", "a", "b", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			me, err := m.GetMetric(tt.key, tt.metricType)
			if tt.err == "" {
				require.NoError(t, err)
				assert.Implements(t, (*Adder)(nil), me)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			}
		})
	}
}
