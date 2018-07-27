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
	"fmt"
	"github.com/pkg/errors"
	"github.com/ricardo-ch/kafka-tools/hitman/command"
	hitman "github.com/ricardo-ch/kafka-tools/hitman/lib"
	"github.com/spf13/cobra"
	"log"
	"os"
	"regexp"
	"strings"
)

type Target struct {
	partition  int32
	offset     int64
	key        string
	valueRegex regexp.Regexp
}

var (
	rootCmd = &cobra.Command{
		Use:     "",
		Short:   "remove a message from kafka",
		Long:    ``,
		PreRunE: preRunCheck,
		RunE:    runCmd,
	}

	brokerList        string
	topic             string
	intermediateTopic string
	createWorkTopic   bool
	commit            bool
	cleanUp           bool
	targetOffset      []string
	targetRegex       string
	targetKey         []string
)

func init() {
	rootCmd.Flags().StringVarP(&brokerList, "brokers", "b", "", "comma separated broker list")
	rootCmd.MarkFlagRequired("broker")

	rootCmd.Flags().StringVarP(&topic, "topic", "t", "", "topic")
	rootCmd.Flags().StringVarP(&intermediateTopic, "iTopic", "i", "", "intermediate topic used for work")

	rootCmd.Flags().StringArrayVarP(&targetOffset, "offset", "o", []string{}, "'partition:offset' couple for targeting messages")
	rootCmd.Flags().StringVarP(&targetRegex, "regex", "r", "", "regex applied on message value for targeting messages")
	rootCmd.Flags().StringArrayVarP(&targetKey, "key", "k", []string{}, "key list for targeting messages")

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

func preRunCheck(cmd *cobra.Command, args []string) error {
	if createWorkTopic {

		counter := 0
		oneOf := []bool{
			cmd.Flags().Changed("offset"),
			cmd.Flags().Changed("key"),
			cmd.Flags().Changed("regex"),
		}
		for i := range oneOf {
			if oneOf[i] {
				counter++
			}
		}
		if counter != 1 {
			return errors.New("should chose one option: offset, regex or key")
		}
	}
	if (createWorkTopic || commit) && !cmd.Flags().Changed("topic") {
		return errors.New("'topic' flag is required for creating work topic or committing")
	}

	return nil
}

func runCmd(cmd *cobra.Command, args []string) error {
	brokers := strings.Split(brokerList, ",")
	if intermediateTopic != "" {
		hitman.SetIntermediateTopic(intermediateTopic)
	}

	var tmpGroupOffset map[string]map[int32]int64
	var err error

	//step 1
	if createWorkTopic {
		contract, err := command.GetContract(targetKey, targetOffset, targetRegex)
		if err != nil {
			return err
		}

		tmpGroupOffset, err = hitman.CreateWorkTopic(brokers, topic, contract)
		if err != nil {
			return err
		}

		err = command.SaveTmpGroupOffset(tmpGroupOffset)
		if err != nil {
			return err
		}
	} else {
		tmpGroupOffset, err = command.LoadTmpGroupOffset()
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
		os.Remove(command.TmpGroupOffsetFile)
	}

	return nil
}
