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
	"fmt"
	"math"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestClient(t *testing.T) {
	Convey("min", t, func() {
		So(min(1, 2), ShouldEqual, 1)
		So(min(2, 1), ShouldEqual, 1)
		So(min(1, -2), ShouldEqual, -2)
	})

	Convey("sanitizeString", t, func() {
		So(sanitizeString(""), ShouldEqual, "")
		So(sanitizeString("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), ShouldEqual, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		So(sanitizeString("abcdefghijklmnopqrstuvwxyz"), ShouldEqual, "abcdefghijklmnopqrstuvwxyz")
		So(sanitizeString("0123456789"), ShouldEqual, "0123456789")
		So(sanitizeString(".-_"), ShouldEqual, ".-_")
		So(sanitizeString(" "), ShouldEqual, "_")
		So(sanitizeString("/foo/${bar}/%*!@"), ShouldEqual, "_foo___bar______")
	})

	Convey("sanitizeMap", t, func() {
		So(sanitizeMap(map[string]string{}), ShouldResemble, map[string]string{})
		So(sanitizeMap(map[string]string{"a b":"/"}), ShouldResemble, map[string]string{"a_b":"_"})
	})

	Convey("NewAtlasClient", t, func() {
		client := NewAtlasClient("/api/v1/publish", map[string]string{
			"nf.app": "foo",
			"bar":    "value with spaces",
		}).(httpAtlasClient)

		expected := map[string]string{
			"nf.app": "foo",
			"bar":    "value_with_spaces",
		}

		So(client.commonTags, ShouldResemble, expected)
	})

	Convey("sendToAtlas", t, func() {
		client := NewAtlasClient("/api/v1/publish", map[string]string{
			"nf.app": "foo",
		}).(httpAtlasClient)

		var payload []byte
		f := func (data []byte) error {
			payload = data
			return nil
		}

		envelope := "{\"tags\":{\"nf.app\":\"foo\"},\"metrics\":%s}"

		client.sendToAtlas([]Metric{}, f)
		So(string(payload), ShouldResemble, fmt.Sprintf(envelope, "[]"))

		client.sendToAtlas([]Metric{
			Metric{
				map[string]string{
					"name": "bar",
				},
				0,
				42.0,
			},
		}, f)
		So(string(payload), ShouldResemble,
			fmt.Sprintf(envelope, "[{\"tags\":{\"name\":\"bar\"},\"timestamp\":0,\"value\":42}]"))
	})

	Convey("sendToAtlas filter NaN values", t, func() {
		client := NewAtlasClient("/api/v1/publish", map[string]string{
			"nf.app": "foo",
		}).(httpAtlasClient)

		var payload []byte
		f := func (data []byte) error {
			payload = data
			return nil
		}

		envelope := "{\"tags\":{\"nf.app\":\"foo\"},\"metrics\":%s}"

		client.sendToAtlas([]Metric{
			Metric{
				map[string]string{
					"name": "notANumber",
				},
				0,
				math.NaN(),
			},
			Metric{
				map[string]string{
					"name": "positiveInfinity",
				},
				0,
				math.Inf(1),
			},
			Metric{
				map[string]string{
					"name": "negativeInfinity",
				},
				0,
				math.Inf(-1),
			},
		}, f)
		So(string(payload), ShouldResemble, fmt.Sprintf(envelope, "[]"))
	})

}

