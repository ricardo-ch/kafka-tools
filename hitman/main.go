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
	"bufio"
	"encoding/json"
	"fmt"
	hitman "github.com/ricardo-ch/kafka-tools/hitman/lib"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const tmpGroupOffsetFile = "tmpGroupOffset"

type Target struct {
	partition int32
	offset    int64
}

var (
	rootCmd = &cobra.Command{
		Use:   "",
		Short: "remove a message from kafka",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			brokers := strings.Split(brokerList, ",")
			if intermediateTopic != "" {
				hitman.SetIntermediateTopic(intermediateTopic)
			}

			var tmpGroupOffset map[string]map[int32]int64
			var err error

			//step 1
			if createWorkTopic {
				tmpGroupOffset, err = hitman.CreateWorkTopic(brokers, topic, func(partition int32, offset int64) bool {
					return partition == target.partition && offset == target.offset
				})
				if err != nil {
					return err
				}

				err = saveTmpGroupOffset(tmpGroupOffset)
				if err != nil {
					return err
				}
			} else {
				tmpGroupOffset, err = loadTmpGroupOffset()
				if err != nil {
					return err
				}
			}

			//step 2
			if commit {
				if createWorkTopic {
					if !askForConfirmation("you are about to delete all messages from your topic and create a bunch of new one, are you sure?") {
						return nil
					}
				}

				err := hitman.Commit(brokers, topic, tmpGroupOffset)
				if err != nil {
					return err
				}
			}

			//step 3
			if cleanUp {
				if commit {
					if !askForConfirmation("All that will be left is what you have in your topic, are you sure?") {
						return nil
					}
				}

				err := hitman.CleanUp(brokers)
				if err != nil {
					return err
				}
				os.Remove(tmpGroupOffsetFile)
			}

			return nil
		},
	}

	brokerList        string
	topic             string
	intermediateTopic string
	target            Target
	createWorkTopic   bool
	commit            bool
	cleanUp           bool
)

func init() {
	rootCmd.Flags().StringVarP(&brokerList, "brokers", "b", "", "comma separated broker list")
	rootCmd.MarkFlagRequired("broker")

	rootCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	rootCmd.MarkFlagRequired("topic")

	rootCmd.Flags().StringVarP(&intermediateTopic, "iTopic", "i", "", "intermediate topic used for work")

	rootCmd.Flags().Int32VarP(&target.partition, "partition", "p", 0, "partition to target")
	rootCmd.MarkFlagRequired("partition") //For now
	rootCmd.Flags().Int64VarP(&target.offset, "offset", "o", 0, "offset to target")
	rootCmd.MarkFlagRequired("offset") //For now

	rootCmd.Flags().BoolVarP(&createWorkTopic, "workTopic", "w", false, "step1: create a temporary topic in expected state")
	rootCmd.Flags().BoolVarP(&commit, "commit", "c", false, "step2: commit the temporary topic into your original topic")
	rootCmd.Flags().BoolVarP(&cleanUp, "cleanup", "l", false, "step3: remove work topic")

}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

func saveTmpGroupOffset(tmpGroupOffset map[string]map[int32]int64) error {
	output, err := json.Marshal(tmpGroupOffset)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(tmpGroupOffsetFile, output, 0644)
}

func loadTmpGroupOffset() (map[string]map[int32]int64, error) {
	input, err := ioutil.ReadFile(tmpGroupOffsetFile)
	if err != nil {
		return nil, err
	}
	tmpGroupOffset := map[string]map[int32]int64{}
	err = json.Unmarshal(input, &tmpGroupOffset)
	return tmpGroupOffset, err
}

func askForConfirmation(s string) bool {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("%s [y/n]: ", s)

		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "y" || response == "yes" {
			return true
		} else if response == "n" || response == "no" {
			return false
		}
	}
}
