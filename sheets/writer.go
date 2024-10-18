// Copyright © 2022 Meroxa, Inc. & Gophers Lab Technologies Pvt. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sheets

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const insertDataOption = "INSERT_ROWS"

type Writer struct {
	// instance of sheets service, used to interact with Google Sheets APIs
	sheetSvc *sheets.Service
	// name of the sheet to write to, required for writing API
	sheetName string
	// spreadsheet ID of the Google sheet
	spreadsheetID string
	// valueInputOption defines whether the data is to be inserted in USER_ENTERED mode or RAW mode
	valueInputOption string
	// maxRetries is the maximum retries to be made before returning an error, in case of 429(rate-limit exceeded error)
	maxRetries uint64
	// the number of unsuccessful retries made with error 429, since last successful data write
	retryCount uint64
}

func NewWriter(
	ctx context.Context,
	oauthCfg *oauth2.Config,
	token *oauth2.Token,
	spreadsheetID, sheetName, valueInputOption string,
	retries uint64,
) (*Writer, error) {
	sheetService, err := sheets.NewService(ctx, option.WithHTTPClient(oauthCfg.Client(ctx, token)))
	if err != nil {
		return nil, fmt.Errorf("error creating sheets(%s) service client: %w", sheetName, err)
	}
	return &Writer{
		spreadsheetID:    spreadsheetID,
		sheetSvc:         sheetService,
		sheetName:        sheetName,
		valueInputOption: valueInputOption,
		maxRetries:       retries,
	}, nil
}

// Write function writes the records to google sheet
func (w *Writer) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	rows := make([][]any, 0)

	for i := range records {
		var (
			row []any
			err error
		)

		// destination connector doesn't support update or delete operations.
		if records[i].Operation == opencdc.OperationDelete || records[i].Operation == opencdc.OperationUpdate {
			continue
		}

		// transform from map to row.
		row, err = transformRecordToRow(records[i])
		if err != nil {
			sdk.Logger(ctx).Debug().Err(err)

			// check if payload is slice.
			row, err = transformFromRow(records[i].Payload.After)
			if err != nil {
				return i, err
			}
		}

		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	// KeyValueInputOption is the config name for how the input data
	// should be interpreted.
	// Creating a google-sheet format to append to google-sheet

	sheetValueFormat := &sheets.ValueRange{
		MajorDimension: majorDimension,
		Range:          w.sheetName,
		Values:         rows,
	}

	_, err := w.sheetSvc.Spreadsheets.Values.Append(
		w.spreadsheetID, w.sheetName,
		sheetValueFormat).ValueInputOption(
		w.valueInputOption).InsertDataOption(
		insertDataOption).Context(ctx).Do()

	if err != nil {
		// retry mechanism, in case of rate limit exceeded error (429)
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == http.StatusTooManyRequests {
			if w.retryCount >= w.maxRetries {
				return 0, fmt.Errorf("rate limit exceeded, retries: %d, error: %w", w.retryCount, err)
			}
			w.retryCount++
			// if retry count doesn't exceed maxRetries, retry with exponential back off
			// block till write either succeeds or all retries are exhausted
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Duration(w.retryCount) * time.Second): // exponential back off
				return w.Write(ctx, records)
			}
		}
		return 0, fmt.Errorf("appending rows to sheet(%s) failed: %w", w.sheetName, err)
	}

	w.retryCount = 0

	return len(records), nil
}

func transformFromRow(data opencdc.Data) ([]any, error) {
	rowArr := make([]interface{}, 0)

	err := json.Unmarshal(data.Bytes(), &rowArr)
	if err != nil {
		return rowArr, fmt.Errorf("unable to marshal the record: %w", err)
	}

	return rowArr, nil
}

func transformRecordToRow(record opencdc.Record) ([]any, error) {
	data, err := structurizeData(record.Payload.After)
	if err != nil {
		return nil, fmt.Errorf("structurize data: %w", err)
	}

	if data == nil {
		return nil, ErrEmptyPayload
	}

	// sort by keys
	keys := make([]string, 0, len(data))

	for k := range data {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	row := make([]any, 0)
	for _, k := range keys {
		row = append(row, data[k])
	}

	return row, err
}

// structurizeData converts opencdc.Data to opencdc.StructuredData.
func structurizeData(data opencdc.Data) (opencdc.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(opencdc.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}
