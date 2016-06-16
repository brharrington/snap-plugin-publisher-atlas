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
	"math"
	"testing"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/core"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAtlasPublisher(t *testing.T) {

	Convey("substitute", t, func() {
		vars := map[string]string{}
		So(substitute("no matches {foo}", vars), ShouldResemble, "no matches {foo}")

		vars["foo"] = "bar"
		So(substitute("single match {foo}", vars), ShouldResemble, "single match bar")
		So(substitute("{foo} multi match {foo}", vars), ShouldResemble, "bar multi match bar")

		vars["0"] = "positional"
		So(substitute("multi var {foo}.{0}", vars), ShouldResemble, "multi var bar.positional")
	})

	Convey("createAtlasTags", t, func() {
		actual := createAtlasTags(core.NewNamespace("test", "foo"), map[string]string{})
		expected := map[string]string{
			"name": "test.foo",
		}
		So(actual, ShouldResemble, expected)

		// ignore plugin_running_on
		actual = createAtlasTags(core.NewNamespace("test", "foo"), map[string]string{
			"plugin_running_on": "foo",
		})
		So(actual, ShouldResemble, expected)

		// ignore unit
		actual = createAtlasTags(core.NewNamespace("test", "foo"), map[string]string{
			"unit": "foo",
		})
		So(actual, ShouldResemble, expected)

		// name override
		actual = createAtlasTags(core.NewNamespace("test", "foo"), map[string]string{
			"name": "custom.name",
		})
		expected = map[string]string{
			"name": "custom.name",
		}
		So(actual, ShouldResemble, expected)

		// other tags
		actual = createAtlasTags(core.NewNamespace("test", "foo"), map[string]string{
			"name": "custom.name",
			"nf.region": "us-east-1",
			"nf.app": "my_app",
		})
		expected = map[string]string{
			"name": "custom.name",
			"nf.region": "us-east-1",
			"nf.app": "my_app",
		}
		So(actual, ShouldResemble, expected)

		// positional vars
		actual = createAtlasTags(core.NewNamespace("test", "foo"), map[string]string{
			"name": "{1}.{0}",
		})
		expected = map[string]string{
			"name": "foo.test",
		}
		So(actual, ShouldResemble, expected)

		// positional vars, from end of namespace
		actual = createAtlasTags(core.NewNamespace("test", "foo"), map[string]string{
			"name": "{-1}.{-2}",
		})
		expected = map[string]string{
			"name": "foo.test",
		}
		So(actual, ShouldResemble, expected)

		// dynamic elements in namespace
		dynNamespace := core.NewNamespace("test").
			AddDynamicElement("host", "desc").
			AddStaticElement("foo")
		dynNamespace[1].Value = "i-12345"
		actual = createAtlasTags(dynNamespace, map[string]string{
		  "name": "{namespace_static}",
		  "fqdn": "{namespace}",
		  "node": "{host}",
		})
		expected = map[string]string{
			"name": "test.foo",
			"fqdn": "test.i-12345.foo",
			"node": "i-12345",
		}
		So(actual, ShouldResemble, expected)
	})

	Convey("convertToBaseUnit", t, func() {
		So(convertToBaseUnit("none", 1e10), ShouldResemble, 1e10)

		So(convertToBaseUnit("ns", 1e10), ShouldResemble, 10.0)
		So(convertToBaseUnit("us", 1e10), ShouldResemble, 1e4)
		So(convertToBaseUnit("ms", 1e10), ShouldResemble, 1e7)

		So(convertToBaseUnit("k", 1e10), ShouldResemble, 1e13)
		So(convertToBaseUnit("M", 1e10), ShouldResemble, 1e16)
		So(convertToBaseUnit("G", 1e10), ShouldResemble, 1e19)
		So(convertToBaseUnit("T", 1e10), ShouldResemble, 1e22)
		So(convertToBaseUnit("P", 1e10), ShouldResemble, 1e25)
		So(convertToBaseUnit("E", 1e10), ShouldResemble, 1e28)
		So(convertToBaseUnit("Z", 1e10), ShouldResemble, 1e31)
		So(convertToBaseUnit("Y", 1e10), ShouldResemble, 1e34)

		So(convertToBaseUnit("Ki", 1), ShouldResemble, 1024.0)
		So(convertToBaseUnit("Mi", 1), ShouldResemble, 1024.0 * 1024.0)
		So(convertToBaseUnit("Gi", 1), ShouldResemble, 1024.0 * 1024.0 * 1024.0)
		So(convertToBaseUnit("Ti", 1), ShouldResemble, math.Pow(1024.0, 4))
		So(convertToBaseUnit("Pi", 1), ShouldResemble, math.Pow(1024.0, 5))
		So(convertToBaseUnit("Ei", 1), ShouldResemble, math.Pow(1024.0, 6))
		So(convertToBaseUnit("Zi", 1), ShouldResemble, math.Pow(1024.0, 7))
		So(convertToBaseUnit("Yi", 1), ShouldResemble, math.Pow(1024.0, 8))
	})

	Convey("toAtlasMetric", t, func() {
		timestamp := time.Now()
		input := *plugin.NewMetricType(core.NewNamespace("foo"), timestamp, nil, "", 99)

		expected := Metric{
			map[string]string{
				"name": "foo",
			},
			uint64(timestamp.Unix() * 1000),
			99.0,
		}

		So(*toAtlasMetric(input), ShouldResemble, expected)
	})

	Convey("toAtlasMetric unit conversion", t, func() {
		timestamp := time.Now()
		input := *plugin.NewMetricType(
			core.NewNamespace("foo"),
			timestamp,
			map[string]string{
				"unit": "Ki",
			},
			"",
			99)

		expected := Metric{
			map[string]string{
				"name": "foo",
			},
			uint64(timestamp.Unix() * 1000),
			99.0 * 1024.0,
		}

		So(*toAtlasMetric(input), ShouldResemble, expected)
	})

	Convey("toAtlasMetric non-numeric", t, func() {
		timestamp := time.Now()
		input := *plugin.NewMetricType(core.NewNamespace("foo"), timestamp, nil, "", "99")
		So(toAtlasMetric(input), ShouldEqual, nil)
	})

	Convey("toAtlasMetrics", t, func() {

		timestamp := time.Now()
		input := []plugin.MetricType{
			*plugin.NewMetricType(core.NewNamespace("foo"), timestamp, nil, "", 99),
		}

		expected := []Metric{
			Metric{
				map[string]string{
					"name": "foo",
				},
				uint64(timestamp.Unix() * 1000),
				99.0,
			},
		}

		So(toAtlasMetrics(input), ShouldResemble, expected)
	})
}
