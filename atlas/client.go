/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package atlas

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

// Maximum number of datapoints to send to Atlas per request.
const metricBatchSize = 10000

type Metric struct {
	Tags map[string]string `json:"tags"`
	Timestamp uint64       `json:"timestamp"`
	Value float64          `json:"value"`
}

type metricBatch struct {
	Tags map[string]string `json:"tags"`
	Metrics []Metric       `json:"metrics"`
}

type AtlasClient interface {
	Publish(metrics []Metric)
}

type httpAtlasClient struct {
	uri string
	commonTags map[string]string
}

// Create a new instance of an Atlas client using HTTP to talk to the default
// publish endpoint.
//
// - uri: the full uri to use for the POST request to Atlas.
// - commonTags: tags that should be applied to all datapoints being sent. This
//   is typically used for infrastructure tags like the cluster and node.
func NewAtlasClient(uri string, commonTags map[string]string) AtlasClient {
	return httpAtlasClient{uri, sanitizeMap(commonTags)}
}

// Helper for finding the minimum value of two integers. The built in
// math library only supports float64.
func min(v1, v2 int) int {
	if v1 <= v2 {
		return v1
	} else {
		return v2
	}
}

// Cleanup the input string. The only allowed characters are
// [A-Za-z0-9_.-]. Others will get converted to an '_'.
func sanitizeString(s string) string {
	copy := make([]uint8, len(s))
	for i := range s {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			copy[i] = c
		case c >= 'A' && c <= 'Z':
			copy[i] = c
		case c >= 'a' && c <= 'z':
			copy[i] = c
		case c == '.' || c == '-':
			copy[i] = c
		default:
			copy[i] = '_'
		}
	}
	return string(copy)
}

// Returns a new map after sanitizing both the keys and the values.
func sanitizeMap(tags map[string]string) map[string]string {
	copy := make(map[string]string)
	for k, v := range tags {
		copy[sanitizeString(k)] = sanitizeString(v)
	}
	return copy
}

// Send all metrics in the array to the Atlas backend.
func (client httpAtlasClient) Publish(metrics []Metric) {
	f := func (data []byte) error {
		response, err := http.Post(client.uri, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		} else if response.StatusCode != 200 {
			msg := fmt.Sprintf("status code %d", response.StatusCode)
			body, err := ioutil.ReadAll(response.Body)
			if err != nil {
				msg = fmt.Sprintf("%s: %s", msg, string(body))
			}
			return errors.New(msg)
		}
		return nil
	}
	client.publish(metrics, f)
}

// Breakup the input array into batches and send them to Atlas.
func (client httpAtlasClient) publish(metrics []Metric, doPost func([]byte) error) {
	logger := log.New()
	n := len(metrics)
	if n == 0 {
		logger.Infof("empty metric list, nothing to send")
	} else {
		logger.Infof("sending %d metrics to %s", n, client.uri)
		for i := 0; i < n; i += metricBatchSize {
			end := min(i + metricBatchSize, n)
			sanitizedBatch := make([]Metric, end - i)
			for j := range metrics[i:end] {
				sanitizedBatch[j] = Metric{
					sanitizeMap(metrics[j].Tags),
					metrics[j].Timestamp,
					metrics[j].Value,
				}
			}
			client.sendToAtlas(sanitizedBatch, doPost)
		}
	}
}

// Filter out floating point values like that are not supported by standard json
// like infinity and NaN.
func (client httpAtlasClient) filterNumbers(metrics []Metric) []Metric {
	buffer := make([]Metric, 0)
	for _, m := range metrics {
		if (!math.IsNaN(m.Value) && !math.IsInf(m.Value, 0)) {
			buffer = append(buffer, m)
		}
	}
	return buffer
}

// Encode the data as json and send to the backend.
func (client httpAtlasClient) sendToAtlas(metrics []Metric, doPost func([]byte) error) {
	logger := log.New()

	batch := metricBatch{client.commonTags, client.filterNumbers(metrics)}
	json, err := json.Marshal(batch)
	if err != nil {
		logger.Errorf("failed to encode metrics as json: %v", err)
	}

	err = doPost(json)
	if err != nil {
		logger.Errorf("post to %v failed: %v", client.uri, err)
	} else {
		logger.Infof("successfully sent %d metrics to %s", len(metrics), client.uri)
	}
}
