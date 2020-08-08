package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	//MetricsAddressCtxKey defines key for context value of the addres for server
	MetricsAddressCtxKey = "metricsServerAddress"
)

// RunServer - run  server on the provided address and expose /metrics endpoint
// return 2 channels: first for getting error messages and second channel idenifies status of the server
// if second channel will be closed - server exited
// Context should contain under key "serverAddressMetrics" string with local address to which it will be binded
// If context cancelled - server also exits
func RunServer(ctx context.Context) (<-chan error, <-chan struct{}) {
	// make this channel buffered to not block execution on writing
	// and values still could be read from closed channel
	chanErr := make(chan error, 1)
	chanSrvExit := make(chan struct{})

	addr, err := getAddressFromContext(ctx)
	if err != nil {
		defer func() {
			chanErr <- fmt.Errorf("Error in server address for metrics: %w", err)
			// we did not start goroutine yet.
			// it is required to close both channels here
			close(chanErr)
			close(chanSrvExit)
		}()
		return chanErr, chanSrvExit
	}
	//run server
	go func() {
		defer close(chanErr)
		defer close(chanSrvExit)
		chanSrvExitInner := make(chan struct{})
		s := getServer(ctx, addr)
		var err error
		go func() {
			defer close(chanSrvExitInner)
			err = s.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				chanErr <- err
			}
		}()

		// block goroutine unless server exited
		select {
		// server requested to stop
		case <-ctx.Done():
			s.Close()
			//once we in this section - need to wait until this channel will be closed
			<-chanSrvExitInner
		// server exited and error was already reported through the error channel
		case <-chanSrvExitInner:
		}
		//it is safe to exit now
	}()
	return chanErr, chanSrvExit
}

func getAddressFromContext(ctx context.Context) (string, error) {
	addrRaw := ctx.Value(MetricsAddressCtxKey)
	if addrRaw == nil {
		return "", fmt.Errorf("Provided context does not contain key '%s'", MetricsAddressCtxKey)
	}
	addr, ok := addrRaw.(string)
	if !ok || addr == "" {
		return "", fmt.Errorf("Provided context does not contain string value in key '%s'", MetricsAddressCtxKey)
	}
	return addr, nil
}

func getServer(ctx context.Context, addr string) *http.Server {
	router := chi.NewRouter()
	router.Get("/metrics", promhttp.Handler().(http.HandlerFunc))
	return &http.Server{
		ReadTimeout:       5 * time.Millisecond,
		WriteTimeout:      5 * time.Millisecond,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Millisecond,
		Addr:              addr,
		TLSConfig:         nil,
		Handler:           router,
		MaxHeaderBytes:    1 << 8,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
}
