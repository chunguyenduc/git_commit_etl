package http

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	"time"
)

type Client struct {
	client  *http.Client
	headers http.Header
	baseUrl string
}

type Options struct {
	timeout time.Duration
	headers http.Header
}

type OptionFunc func(*Options)

func WithTimeout(timeout time.Duration) OptionFunc {
	return func(o *Options) {
		o.timeout = timeout
	}
}

func WithBearerToken(token string) OptionFunc {
	return func(o *Options) {
		if o.headers == nil {
			o.headers = http.Header{}
		}
		o.headers.Set("Authorization", "Bearer "+token)
	}
}

func WithHeader(key, value string) OptionFunc {
	return func(o *Options) {
		if o.headers == nil {
			o.headers = http.Header{}
		}
		o.headers.Add(key, value)
	}
}

func NewClient(baseUrl string, opts ...OptionFunc) *Client {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.timeout == 0 {
		options.timeout = 30 * time.Second
	}

	client := &http.Client{
		Timeout: options.timeout,
	}

	return &Client{
		client:  client,
		baseUrl: baseUrl,
		headers: options.headers,
	}
}

func (c *Client) Get(ctx context.Context, path string, params map[string]string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseUrl+path, nil)
	if err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to create Get request")
		return nil, err
	}

	if c.headers != nil {
		request.Header = c.headers
	}

	query := request.URL.Query()
	for key, value := range params {
		query.Add(key, value)
	}
	request.URL.RawQuery = query.Encode()

	response, err := c.client.Do(request)
	if err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to send Get request")
		return nil, err
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Ctx(ctx).Err(err).Msg("Failed to read response body")
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("error Get request status code: %s", response.Status)
		log.Ctx(ctx).Err(err).Send()
		return nil, err
	}

	return body, nil
}
