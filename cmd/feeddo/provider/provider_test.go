package provider

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStream(t *testing.T) {
	tests := []struct {
		name      string
		URL       string
		err       string
		isFile    bool
		runServer bool
	}{
		{
			name:      "non existing file",
			URL:       "file:///test.xml",
			err:       "Unable to read file `file:///test.xml` because of open /test.xml: no such file or directory",
			isFile:    true,
			runServer: false,
		},
		{
			name:      "file success",
			URL:       "file://testdata/one_item.xml",
			err:       "",
			isFile:    true,
			runServer: false,
		},
		{
			name:      "wrong url",
			URL:       "http://localhost:8945",
			err:       "connect: connection refused",
			isFile:    false,
			runServer: false,
		},
		{
			name:      "success download",
			URL:       "",
			err:       "",
			isFile:    false,
			runServer: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.runServer {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprintln(w, "Hello, there")
				}))
				defer ts.Close()

				tt.URL = ts.URL
			}

			u, err := url.Parse(tt.URL)
			require.NoError(t, err)
			stream, err := CreateStream(u)
			if stream != nil {
				defer stream.Close()
			}
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, stream)
				if tt.isFile {
					assert.IsType(t, &os.File{}, stream)
				} else {
					_, ok := stream.(io.ReadCloser)
					assert.True(t, ok)
				}
			}
		})
	}
}
