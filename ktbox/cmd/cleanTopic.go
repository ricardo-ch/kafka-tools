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

package cmd

import (
	"github.com/ricardo-ch/kafka-tools/ktbox/lib"
	"github.com/spf13/cobra"
	"strconv"
)

// cleanTopicCmd represents the cleanTopic command
var (
	cleanTopicCmd = &cobra.Command{
		Use:   "clean-topic",
		Short: "Remove messages from topic",
		RunE: func(cmd *cobra.Command, args []string) error {
			var partitionList []int32
			for _, partition := range partitions {
				partitionID, err := strconv.Atoi(partition)
				if err != nil {
					return err
				}
				partitionList = append(partitionList, int32(partitionID))
			}

			client, err := lib.NewClient(getBrokerList())
			if err != nil {
				return err
			}
			err = lib.CleanTopic(client, topic, partitionList)
			if err != nil {
				return err
			}
			return nil
		},
	}

	topic      string
	partitions []string
)

func init() {
	rootCmd.AddCommand(cleanTopicCmd)

	cleanTopicCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	cleanTopicCmd.MarkFlagRequired("topic")

	cleanTopicCmd.Flags().StringArrayVarP(&partitions, "partitions", "p", nil, "partitions")
}
