// Copyright © 2018 Camunda Services GmbH (info@camunda.com)
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

package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/zeebe-io/zeebe/clients/go/zbc"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const (
	DefaultAddressHost = "127.0.0.1"
	DefaultAddressPort = 26500
)

var client zbc.ZBClient

var addressFlag string
var caCertPathFlag string
var useOAuthFlag bool
var clientIdFlag string
var clientSecretFlag string
var audienceFlag string
var authzUrlFlag string
var insecureFlag bool

var rootCmd = &cobra.Command{
	Use:   "zbctl",
	Short: "zeebe command line interface",
	Long: `zbctl is command line interface designed to create and read resources inside zeebe broker. 
It is designed for regular maintenance jobs such as:
	* deploying workflows,
	* creating jobs and workflow instances
	* activating, completing or failing jobs
	* update variables and retries
	* view cluster status`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&addressFlag, "address", "", "Specify a contact point address")
	rootCmd.PersistentFlags().StringVar(&caCertPathFlag, "certPath", "", "Specify a path to a certificate with which to validate gateway requests")
	rootCmd.PersistentFlags().BoolVar(&useOAuthFlag, "useOAuth", false, "Specify if OAuth credentials should be used. Automatically enabled if "+
		"at least one of the following is provided:\n  * clientId\n  * clientSecret\n  * audience\n  * authzUrl")
	rootCmd.PersistentFlags().StringVar(&clientIdFlag, "clientId", "", "Specify a client identifier to request an access token. Can be overridden by the environment variable 'ZEEBE_CLIENT_ID'")
	rootCmd.PersistentFlags().StringVar(&clientSecretFlag, "clientSecret", "", "Specify a client secret to request an access token. Can be overridden by the environment variable 'ZEEBE_CLIENT_SECRET'")
	rootCmd.PersistentFlags().StringVar(&audienceFlag, "audience", "", "Specify the resource that the access token should be valid for. Can be overridden by the environment variable 'ZEEBE_TOKEN_AUDIENCE'")
	rootCmd.PersistentFlags().StringVar(&authzUrlFlag, "authzUrl", "", "Specify an authorization server URL from which to request an access token. Can be overridden by the environment variable 'ZEEBE_AUTHORIZATION_SERVER_URL'")
	rootCmd.PersistentFlags().BoolVar(&insecureFlag, "insecure", false, "Specify if zbctl should use an unsecured connection")
}

// initClient will create a client with in the following precedence: address flag, environment variable, default address
var initClient = func(cmd *cobra.Command, args []string) error {
	var err error

	address := parseAddress()

	credsProvider, err := parseCredentials()
	if err != nil {
		return err
	}

	client, err = zbc.NewZBClient(&zbc.ZBClientConfig{
		GatewayAddress:         address,
		UsePlaintextConnection: insecureFlag,
		CaCertificatePath:      caCertPathFlag,
		CredentialsProvider:    credsProvider,
	})

	return err
}

func parseAddress() string {
	address := DefaultAddressHost

	addressEnv := os.Getenv("ZEEBE_ADDRESS")
	if len(addressEnv) > 0 {
		address = addressEnv
	}

	if len(addressFlag) > 0 {
		address = addressFlag
	}
	address = appendPort(address)

	return address
}

func parseCredentials() (credsProvider zbc.CredentialsProvider, err error) {
	if useOAuthFlag || clientIdFlag != "" || clientSecretFlag != "" || audienceFlag != "" || authzUrlFlag != "" {
		return zbc.NewOAuthCredentialsProvider(&zbc.OAuthProviderConfig{
			ClientID:               clientIdFlag,
			ClientSecret:           clientSecretFlag,
			Audience:               audienceFlag,
			AuthorizationServerURL: authzUrlFlag,
		})
	}

	return nil, nil
}

func keyArg(key *int64) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("expects key as only positional argument")
		}

		value, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid argument %q for %q: %s", args[0], "key", err)
		}

		*key = value

		return nil
	}
}

func printJson(value interface{}) error {
	valueJson, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		fmt.Println(string(valueJson))
	}
	return err
}

func appendPort(address string) string {
	if strings.Contains(address, ":") {
		return address
	} else {
		return fmt.Sprintf("%s:%d", address, DefaultAddressPort)
	}
}
