// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
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

package main

import (
	"github.com/spf13/cobra"
	"log"
	"strings"
)

type Target struct {
	partition int32
	offset    int64
}

// rootCmd represents the base command when called without any subcommands
var (
	rootCmd = &cobra.Command{
		Use:   "",
		Short: "remove a message from kafka",
		Long:  ``,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		RunE: func(cmd *cobra.Command, args []string) error {
			brokers := strings.Split(brokerList, ",")

			err := KillMessage(brokers, topic, func(partition int32, offset int64) bool {
				return partition == target.partition && offset == target.offset
			})

			return err
		},
	}

	brokerList        string
	topic             string
	intermediateTopic string
	target            Target
)

func init() {
	rootCmd.Flags().StringVarP(&brokerList, "brokers", "b", "", "comma separated broker list")
	rootCmd.MarkFlagRequired("broker")

	rootCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	rootCmd.MarkFlagRequired("topic")

	rootCmd.Flags().StringVarP(&intermediateTopic, "iTopic", "i", "kafka-hitman-work", "intermediate topic used for work")

	rootCmd.Flags().Int32VarP(&target.partition, "partition", "p", 0, "partition to target")
	rootCmd.MarkFlagRequired("partition") //For now
	rootCmd.Flags().Int64VarP(&target.offset, "offset", "o", 0, "offset to target")
	rootCmd.MarkFlagRequired("offset") //For now

}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}
