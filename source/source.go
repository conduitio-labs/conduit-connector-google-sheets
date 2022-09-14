/*
Copyright © 2022 Meroxa, Inc. & Gophers Lab Technologies Pvt. Ltd.

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

package source

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-google-sheets/config"
	"github.com/conduitio-labs/conduit-connector-google-sheets/sheets"
	"github.com/conduitio-labs/conduit-connector-google-sheets/source/iterator"
	"github.com/conduitio-labs/conduit-connector-google-sheets/source/position"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Source connector
type Source struct {
	sdk.UnimplementedSource

	iterator Iterator
	conf     Config
}

type Iterator interface {
	HasNext() bool
	Next(ctx context.Context) (sdk.Record, error)
	Stop(ctx context.Context)
}

func NewSource() sdk.Source {
	return &Source{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyCredentialsFile: {
			Default:     "",
			Required:    true,
			Description: "path to credentials.json file used",
		},
		config.KeyTokensFile: {
			Default:     "",
			Required:    true,
			Description: "path to token.json file containing a json with atleast refresh_token.",
		},
		config.KeySheetURL: {
			Default:     "",
			Required:    true,
			Description: "Google sheet url to fetch the records from",
		},
		KeyPollingPeriod: {
			Default:     "6s",
			Required:    false,
			Description: "Time interval for consecutive fetching data.",
		},
		KeyDateTimeRenderOption: {
			Default:     "FORMATTED_STRING",
			Required:    false,
			Description: "Format of the Date/time related values. Valid values: SERIAL_NUMBER, FORMATTED_STRING",
		},
		KeyValueRenderOption: {
			Default:     "FORMATTED_VALUE",
			Required:    false,
			Description: "Format of the dynamic/reference data. Valid values: FORMATTED_VALUE, UNFORMATTED_VALUE, FORMULA",
		},
	}
}

// Configure validates the passed config and prepares the source connector
func (s *Source) Configure(_ context.Context, cfg map[string]string) error {
	sheetsConfig, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("error parsing source config: %w", err)
	}
	s.conf = sheetsConfig
	return nil
}

// Open prepare the plugin to start sending records from the given position
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	pos, err := position.ParseRecordPosition(rp)
	if err != nil {
		return fmt.Errorf("couldn't parse position: %w", err)
	}

	s.iterator, err = iterator.NewSheetsIterator(ctx, pos,
		sheets.BatchReaderArgs{
			OAuthConfig:          s.conf.OAuthConfig,
			OAuthToken:           s.conf.OAuthToken,
			SpreadsheetID:        s.conf.GoogleSpreadsheetID,
			SheetID:              s.conf.GoogleSheetID,
			DateTimeRenderOption: s.conf.DateTimeRenderOption,
			ValueRenderOption:    s.conf.ValueRenderOption,
			PollingPeriod:        s.conf.PollingPeriod,
		},
	)

	if err != nil {
		return fmt.Errorf("couldn't create a iterator: %w", err)
	}
	return nil
}

// Read gets the next object
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if !s.iterator.HasNext() {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		// Next will return context canceled error, to signal graceful stop, as expected by conduit server
		// in case of other error wrapped errors will be returned
		return sdk.Record{}, err
	}
	return r, nil
}

// Teardown is called by the conduit server to stop the source connector
// all the cleanup should be done in this function
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		s.iterator.Stop(ctx)
	}
	return nil
}

// Ack is called by the conduit server after the record has been successfully processed by all destination connectors
// We do not need to send any ack to Google sheets as we poll the Sheets API for data, so there is no data to be ack'd
func (s *Source) Ack(ctx context.Context, tp sdk.Position) error {
	pos, err := position.ParseRecordPosition(tp)
	if err != nil {
		sdk.Logger(ctx).Error().Err(err).Msg("invalid position received")
	}
	sdk.Logger(ctx).Trace().Int64("row_offset", pos.RowOffset).Msg("message ack received")
	return nil
}
