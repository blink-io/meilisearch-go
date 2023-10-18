package meilisearch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-resty/resty/v2"
)

const (
	contentTypeJSON   string = "application/json"
	contentTypeNDJSON string = "application/x-ndjson"
	contentTypeCSV    string = "text/csv"
)

type internalRequest struct {
	endpoint    string
	method      string
	contentType string

	withRequest     interface{}
	withResponse    interface{}
	withQueryParams map[string]string

	acceptedStatusCodes []int

	functionName string
}

func (c *Client) executeRequest(req internalRequest) error {
	internalError := &Error{
		Endpoint:         req.endpoint,
		Method:           req.method,
		Function:         req.functionName,
		RequestToString:  "empty request",
		ResponseToString: "empty response",
		MeilisearchApiError: meilisearchApiError{
			Message: "empty Meilisearch message",
		},
		StatusCodeExpected: req.acceptedStatusCodes,
	}

	response, err := c.sendRequest(&req, internalError)
	if err != nil {
		return err
	}
	internalError.StatusCode = response.StatusCode()

	err = c.handleStatusCode(&req, response, internalError)
	if err != nil {
		return err
	}

	err = c.handleResponse(&req, response, internalError)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) sendRequest(req *internalRequest, internalError *Error) (*resty.Response, error) {
	var (
		request *resty.Request

		err error
	)

	// Setup URL
	requestURL, err := url.Parse(c.config.Host + req.endpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to parse url: %w", err)
	}

	// Build query parameters
	if req.withQueryParams != nil {
		query := requestURL.Query()
		for key, value := range req.withQueryParams {
			query.Set(key, value)
		}

		requestURL.RawQuery = query.Encode()
	}

	if c.config.Timeout != 0 {
		c.httpClient.SetTimeout(c.config.Timeout)
	}

	request = c.httpClient.R()

	if req.withRequest != nil {
		if req.method == http.MethodGet || req.method == http.MethodHead {
			return nil, fmt.Errorf("sendRequest: request body is not expected for GET and HEAD requests")
		}
		if req.contentType == "" {
			return nil, fmt.Errorf("sendRequest: request body without Content-Type is not allowed")
		}

		rawRequest := req.withRequest
		if bytes, ok := rawRequest.([]byte); ok {
			// If the request body is already a []byte then use it directly
			request.SetBody(bytes)
		} else if reader, ok := rawRequest.(io.Reader); ok {
			// If the request body is an io.Reader then stream it directly until io.EOF
			// NOTE: Avoid using this, due to problems with streamed request bodies
			data, err := io.ReadAll(reader)
			if err != nil {
				return nil, fmt.Errorf("sendRequest: unable to read data from io.Reader")
			}
			request.SetBody(data)
		} else {
			// Otherwise convert it to JSON
			var (
				data []byte
				err  error
			)
			if marshaler, ok := rawRequest.(json.Marshaler); ok {
				data, err = marshaler.MarshalJSON()
			} else {
				data, err = json.Marshal(rawRequest)
			}
			internalError.RequestToString = string(data)
			if err != nil {
				return nil, internalError.WithErrCode(ErrCodeMarshalRequest, err)
			}
			request.SetBody(data)
		}
	}

	// adding request headers
	if req.contentType != "" {
		request.Header.Set("Content-Type", req.contentType)
	}
	if c.config.APIKey != "" {
		request.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	}

	request.Header.Set("User-Agent", GetQualifiedVersion())
	// request is sent

	response, err := request.Execute(req.method, requestURL.String())

	// request execution timeout
	if err != nil && isTimeoutErr(err) {
		return nil, internalError.WithErrCode(MeilisearchTimeoutError, err)
	}
	// request execution fail

	if err != nil {
		return nil, internalError.WithErrCode(MeilisearchCommunicationError, err)
	}

	return response, nil
}

func isTimeoutErr(err error) bool {
	if urlErr, ok := err.(*url.Error); ok {
		return ok && urlErr.Timeout()
	}
	return false
}

func (c *Client) handleStatusCode(req *internalRequest, response *resty.Response, internalError *Error) error {
	if req.acceptedStatusCodes != nil {

		// A successful status code is required so check if the response status code is in the
		// expected status code list.
		for _, acceptedCode := range req.acceptedStatusCodes {
			if response.StatusCode() == acceptedCode {
				return nil
			}
		}
		// At this point the response status code is a failure.
		rawBody := response.Body()

		internalError.ErrorBody(rawBody)

		if internalError.MeilisearchApiError.Code == "" {
			return internalError.WithErrCode(MeilisearchApiErrorWithoutMessage)
		}
		return internalError.WithErrCode(MeilisearchApiError)
	}

	return nil
}

func (c *Client) handleResponse(req *internalRequest, response *resty.Response, internalError *Error) (err error) {
	if req.withResponse != nil {

		// A json response is mandatory, so the response interface{} need to be unmarshal from the response payload.
		rawBody := response.Body()
		internalError.ResponseToString = string(rawBody)

		var err error
		if resp, ok := req.withResponse.(json.Unmarshaler); ok {
			err = resp.UnmarshalJSON(rawBody)
			req.withResponse = resp
		} else {
			err = json.Unmarshal(rawBody, req.withResponse)
		}
		if err != nil {
			return internalError.WithErrCode(ErrCodeResponseUnmarshalBody, err)
		}
	}
	return nil
}
