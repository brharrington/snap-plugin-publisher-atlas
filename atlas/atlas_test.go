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
	"testing"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/core"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAtlasPublisher(t *testing.T) {
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