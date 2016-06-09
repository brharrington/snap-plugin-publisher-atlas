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
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
)

const (
	name       = "atlas"
	version    = 1
	pluginType = plugin.PublisherPluginType
)

type atlasPublisher struct {
}

func NewAtlasPublisher() *atlasPublisher {
	return &atlasPublisher{}
}

// TODO: there is bound to be a better way
func toNumber(v interface{}) (float64, error) {
	switch i := v.(type) {
	case int:
		return float64(i), nil
	case int8:
		return float64(i), nil
	case int16:
		return float64(i), nil
	case int32:
		return float64(i), nil
	case int64:
		return float64(i), nil
	case uint:
		return float64(i), nil
	case uint8:
		return float64(i), nil
	case uint16:
		return float64(i), nil
	case uint32:
		return float64(i), nil
	case uint64:
		return float64(i), nil
	case float32:
		return float64(i), nil
	case float64:
		return float64(i), nil
	default:
		return math.NaN(), errors.New(fmt.Sprintf("not a number: '%v' %T", v, v))
	}
}

// Convert a snap MetricType value to an Atlas metric.
func toAtlasMetrics(metrics []plugin.MetricType) []Metric {
	var atlasMetrics []Metric
	for i := range metrics {
		m := metrics[i]
		name := strings.Join(m.Namespace().Strings(), ".")
		v, err := toNumber(m.Data())
		if err == nil {
			atlasMetrics = append(atlasMetrics, Metric{
				map[string]string{
					"name": name,
				},
				uint64(m.Timestamp().Unix() * 1000),
				v,
			})
		}
	}
	return atlasMetrics
}

func (f *atlasPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	logger := log.New()
	logger.Println("Publishing started")
	var metrics []plugin.MetricType

	uri := config["uri"].(ctypes.ConfigValueStr).Value
	logger.Printf("URI %v", uri)

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&metrics); err != nil {
			logger.Printf("Error decoding: error=%v content=%v", err, content)
			return err
		}
	default:
		logger.Printf("Error unknown content type '%v'", contentType)
		return errors.New(fmt.Sprintf("Unknown content type '%s'", contentType))
	}

	// TODO: support for common tags
	client := NewAtlasClient(uri, map[string]string {})
	client.Publish(toAtlasMetrics(metrics))

	return nil
}

func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(name, version, pluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

func (f *atlasPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {

	r1, err := cpolicy.NewStringRule("uri", true)
	handleErr(err)
	r1.Description = "URI for Atlas server."

	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()
	config.Add(r1)
	cp.Add([]string{""}, config)
	return cp, nil
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
