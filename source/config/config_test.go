/* Copyright © 2022 Meroxa, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package config

type sourceTestCase []struct {
	testCase string
	params   map[string]string
	expected Config
}

//func TestParse(t *testing.T) {
//	cases := sourceTestCase{
//		{
//			testCase: "Checking against default values",
//			params: map[string]string{
//				KeyPollingPeriod: "2m",
//			},
//			expected: Config{},
//		},
//		{
//			testCase: "Checking against default values",
//			params:   map[string]string{},
//			expected: Config{},
//		},
//		{
//			testCase: "Checking against default values",
//			params: map[string]string{
//				KeyPollingPeriod: "",
//			},
//			expected: Config{
//				PollingPeriod: 3 * time.Minute,
//			},
//		},
//		{
//			testCase: "Checking against default values",
//			params: map[string]string{
//				"sheet_id": "32",
//			},
//			expected: Config{
//				PollingPeriod: 3 * time.Minute,
//			},
//		},
//		{
//			testCase: "Checking against default values",
//			params: map[string]string{
//				KeyPollingPeriod: "",
//			},
//			expected: Config{},
//		},
//		{
//			testCase: "Checking against if any required value is empty",
//			params: map[string]string{
//				KeyPollingPeriod: "2m",
//			},
//			expected: Config{},
//		},
//		{
//			testCase: "Checking against random values case",
//			params: map[string]string{
//				KeyPollingPeriod: "2s",
//			},
//			expected: Config{
//				PollingPeriod: 2 * time.Second,
//			},
//		},
//		{
//			testCase: "Checking for IDEAL case - 1",
//			params: map[string]string{
//				KeyPollingPeriod: "2m",
//			},
//			expected: Config{
//				PollingPeriod: 2 * time.Minute,
//			},
//		},
//	}
//
//	for _, tc := range cases {
//		t.Run(tc.testCase, func(t *testing.T) {
//			cfg, err := Parse(tc.params)
//			if err != nil {
//				assert.NotNil(t, err)
//			} else {
//				assert.Equal(t, tc.expected, cfg)
//			}
//		})
//	}
//}
