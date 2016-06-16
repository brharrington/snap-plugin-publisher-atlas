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
	"regexp"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
	"github.com/intelsdi-x/snap/core"
)

const (
	name       = "atlas"
	version    = 1
	pluginType = plugin.PublisherPluginType
)

var ignoredTags = map[string]bool{
	"unit": true,
	"plugin_running_on": true,
}

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

// Normalize to base unit.
func convertToBaseUnit(unit string, value float64) float64 {
	switch unit {
	case "ns": // nanoseconds
		return value / 1e9
	case "us": // microseconds
		return value / 1e6
	case "ms": // milliseconds
		return value / 1e3

	// Metric prefixes
	case "k":
		return value * 1e3
	case "M":
		return value * 1e6
	case "G":
		return value * 1e9
	case "T":
		return value * 1e12
	case "P":
		return value * 1e15
	case "E":
		return value * 1e18
	case "Z":
		return value * 1e21
	case "Y":
		return value * 1e24

	// Binary prefixes
	case "Ki":
		return value * math.Pow(1024.0, 1)
	case "Mi":
		return value * math.Pow(1024.0, 2)
	case "Gi":
		return value * math.Pow(1024.0, 3)
	case "Ti":
		return value * math.Pow(1024.0, 4)
	case "Pi":
		return value * math.Pow(1024.0, 5)
	case "Ei":
		return value * math.Pow(1024.0, 6)
	case "Zi":
		return value * math.Pow(1024.0, 7)
	case "Yi":
		return value * math.Pow(1024.0, 8)

	default:
		return value
	}
}

// Replaces variables in the pattern string with matching values from the
// passed in map. Variables are indicated using braces, e.g.: {varname}.
func substitute(pattern string, vars map[string]string) string {
	tmp := pattern
	for k, v := range vars {
		vname := fmt.Sprintf("{%s}", k)
		tmp = strings.Replace(tmp, vname, v, -1)
	}
	return tmp
}

// Create the Atlas tag map from the tags and namespace of the input
// MetricType.
func createAtlasTags(namespace core.Namespace, tags map[string]string) map[string]string {
	// Convert namespace to variable map
	vars := map[string]string{
		"namespace": strings.Join(namespace.Strings(), "."),
	}
	staticNamespace := []string{}
	n := len(namespace)
	for i := 0; i < n; i++ {
		// Add in positional variables
		vars[fmt.Sprintf("%d", i)] = namespace[i].Value
		vars[fmt.Sprintf("%d", i - n)] = namespace[i].Value

		// For dynamic elements, add in variable with name. Otherwise append
		// to static set.
		if namespace[i].IsDynamic() {
			vars[namespace[i].Name] = namespace[i].Value
		} else {
			staticNamespace = append(staticNamespace, namespace[i].Value)
		}
	}
	vars["namespace_static"] = strings.Join(staticNamespace, ".")

	// By default use the parts of the namespace to form the name. If an explicit
	// 'name' key is used in the tags, then it will overwrite this value.
	atlasTags := map[string]string{
		"name": vars["namespace"],
	}

	// Copy tags that are not explicitly ignored into the Atlas tag map.
	for k, v := range tags {
		if _, ignored := ignoredTags[k]; !ignored {
			atlasTags[k] = substitute(v, vars)
		}
	}

	return atlasTags
}

// Convert a snap MetricType value to an Atlas metric.
func toAtlasMetric(metric plugin.MetricType) *Metric {
	tags := createAtlasTags(metric.Namespace(), metric.Tags())
	v, err := toNumber(metric.Data())
	if err == nil {
		unit, ok := metric.Tags()["unit"]
		if ok {
			v = convertToBaseUnit(unit, v)
		}

		m := Metric{
			tags,
			uint64(metric.Timestamp().Unix() * 1000),
			v,
		}
		return &m
	} else {
		return nil
	}
}

// Convert input metric array to Atlas metric type.
func toAtlasMetrics(metrics []plugin.MetricType) []Metric {
	var atlasMetrics []Metric
	for i := range metrics {
		m := toAtlasMetric(metrics[i])
		if m != nil {
			atlasMetrics = append(atlasMetrics, *m)
		}
	}
	return atlasMetrics
}

// Get the exclude regex or return nil if it is not present or was an invalid
// expression.
func getExclude(logger *log.Logger, config map[string]ctypes.ConfigValue) *regexp.Regexp {
	if cfgValue, ok := config["exclude"]; ok {
		exclude := cfgValue.(ctypes.ConfigValueStr).Value
		r, err := regexp.Compile(exclude)
		if err != nil {
			logger.Warn("failed to compile exclude pattern '%s': %v", exclude, err)
			return nil
		} else {
			return r
		}
	} else {
		return nil
	}
}

// Filter out all metrics that match the regex.
func filterNot(metrics []plugin.MetricType, re *regexp.Regexp) []plugin.MetricType {
	if re == nil {
		return metrics
	} else {
		filtered := []plugin.MetricType{}
		for _, m := range metrics {
			name := m.Namespace().String()
			if !re.MatchString(name) {
				filtered = append(filtered, m)
			}
		}
		return filtered
	}
}

func (f *atlasPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	logger := log.New()
	logger.Println("Publishing started")
	var metrics []plugin.MetricType

	uri := config["uri"].(ctypes.ConfigValueStr).Value
	exclude := getExclude(logger, config)

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

	// Filter and convert to Atlas data model
	atlasMetrics := toAtlasMetrics(filterNot(metrics, exclude))
	client := NewAtlasClient(uri, map[string]string {})
	client.Publish(atlasMetrics)

	return nil
}

func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(name, version, pluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

func (f *atlasPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {

	r1, err := cpolicy.NewStringRule("uri", true)
	handleErr(err)
	r1.Description = "URI for Atlas server."

	r2, err := cpolicy.NewStringRule("exclude", false)
	handleErr(err)
	r2.Description = "Regex on the namespace to exclude certain metrics."

	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()
	config.Add(r1, r2)
	cp.Add([]string{""}, config)
	return cp, nil
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
