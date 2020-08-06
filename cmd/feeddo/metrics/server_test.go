package metrics

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunServerContextError(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		err  string
	}{
		{"No key", context.Background(),
			"Error in server address for metrics: Provided context does not contain key 'serverAddressMetrics'"},
		{"No value", context.WithValue(context.Background(), "serverAddressMetrics", nil),
			"Error in server address for metrics: Provided context does not contain key 'serverAddressMetrics'"},
		{"Empty string", context.WithValue(context.Background(), "serverAddressMetrics", ""),
			"Error in server address for metrics: Provided context does not contain string value in key 'serverAddressMetrics'"},
		{"fake address", context.WithValue(context.Background(), "serverAddressMetrics", "abc:-10050"), "listen tcp: address -10050: invalid port"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chanErr, chanClose := RunServer(tt.ctx)
			// all scenarios should fail need to wat for server to close this channel
			<-chanClose
			err := <-chanErr
			require.Error(t, err)
			assert.Equal(t, tt.err, err.Error())
		})
	}
}

func TestRunServerPortAlreadyInUse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}))
	defer s.Close()
	address := strings.Replace(s.URL, "http://", "", 1)
	ctx := context.WithValue(context.Background(), "serverAddressMetrics", address)
	chanErr, chanClose := RunServer(ctx)
	// this scenario should result in error and this channel should be closed
	<-chanClose
	err := <-chanErr
	require.Error(t, err)
	assert.Equal(t, "listen tcp "+address+": bind: address already in use", err.Error())
}

func TestRunServerHappyPath(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	randPort := rand.Intn(1000) + 55000
	ctx := context.WithValue(context.Background(), "serverAddressMetrics", fmt.Sprintf("127.0.0.1:%d", randPort))
	ctx, cancelFunc := context.WithCancel(ctx)
	chanErr, chanClose := RunServer(ctx)
	//set some timeout
	<-time.After(10 * time.Millisecond)
	//cancel context
	cancelFunc()
	// wait for server to exit
	<-chanClose
	//check error
	err := <-chanErr
	require.NoError(t, err)
}
