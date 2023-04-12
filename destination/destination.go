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

package destination

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-google-sheets/config"
	"github.com/conduitio-labs/conduit-connector-google-sheets/sheets"
)

// Destination connector
type Destination struct {
	sdk.UnimplementedDestination

	// config holds the destination config
	config Config
	// writer is the instance of sheets writer, which is a wrapper over sheets write API
	writer *sheets.Writer
}

func NewDestination() sdk.Destination {
	return &Destination{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyCredentialsFile: {
			Default:     "",
			Required:    true,
			Description: "path to credentials.json file used",
		},
		config.KeyTokensFile: {
			Default:     "",
			Required:    true,
			Description: "path to token.json file containing a json with at least refresh_token.",
		},
		config.KeySheetURL: {
			Default:     "",
			Required:    true,
			Description: "Google sheet url to fetch the records from",
		},
		KeySheetName: {
			Default:     "",
			Required:    true,
			Description: "Google sheet name to fetch the records",
		},
		KeyValueInputOption: {
			Default:     "USER_ENTERED",
			Required:    false,
			Description: "Whether the data be inserted in USER_ENTERED mode or RAW mode",
		},
		KeyMaxRetries: {
			Default:     "3",
			Required:    false,
			Description: "Max API retries to be attempted, in case of 429 error, before returning error",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context,
	cfg map[string]string) error {
	sheetsConfig, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("failed parsing the config: %w", err)
	}

	d.config = Config{
		Config:           sheetsConfig.Config,
		SheetName:        sheetsConfig.SheetName,
		ValueInputOption: sheetsConfig.ValueInputOption,
	}
	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	writer, err := sheets.NewWriter(
		ctx,
		d.config.OAuthConfig,
		d.config.OAuthToken,
		d.config.GoogleSpreadsheetID,
		d.config.SheetName,
		d.config.ValueInputOption,
		d.config.MaxRetries,
	)
	if err != nil {
		return fmt.Errorf("unable to init writer: %w", err)
	}

	d.writer = writer

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	i, err := d.writer.Write(ctx, records)
	if err != nil {
		return i, err
	}

	return len(records), nil
}

// Teardown writes all the pending records to sheets and gracefully disconnects the client
func (d *Destination) Teardown(ctx context.Context) error {
	d.writer = nil

	return nil
}
