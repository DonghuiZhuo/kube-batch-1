/*
Copyright 2018 The Kubernetes Authors.

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

package scheduler

import (
	"fmt"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var defaultSchedulerConf = `
actions:
- name: allocate
- name: backfill
tiers:
- plugins:
  - name: priority
  - name: gang
- plugins:
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`

func loadSchedulerConf(confStr string) (*conf.SchedulerConfiguration, error) {
	schedulerConf := &conf.SchedulerConfiguration{}

	buf := make([]byte, len(confStr))
	copy(buf, confStr)

	if err := yaml.Unmarshal(buf, schedulerConf); err != nil {
		return nil, err
	}

	// Set default settings for each plugin if not set
	for i, tier := range schedulerConf.Tiers {
		for j := range tier.Plugins {
			plugins.ApplyPluginConfDefaults(&schedulerConf.Tiers[i].Plugins[j])
		}
	}

	for i, action := range schedulerConf.Actions {
		if _, found := framework.GetAction(action.Name); !found {
			return nil, fmt.Errorf("failed to found Action %s, ignore it", action.Name)
		}

		if action.Options == nil {
			schedulerConf.Actions[i].Options = map[string]string{}
		}

		// set default value for backfill enabled
		if action.Name == "backfill" {
			if _, found := schedulerConf.Actions[i].Options[conf.BackfillFlagName]; !found {
				schedulerConf.Actions[i].Options[conf.BackfillFlagName] = "false"
			}
		}
	}

	return schedulerConf, nil
}

func getActions(schedulerConf *conf.SchedulerConfiguration) ([]framework.Action, map[string]map[string]string, error) {
	var actions []framework.Action
	var actionOptions = map[string]map[string]string{}

	for _, confAction := range schedulerConf.Actions {
		var action framework.Action
		var found bool
		if action, found = framework.GetAction(confAction.Name); !found {
			return nil, nil, fmt.Errorf("failed to found Action %s, ignore it", action.Name())
		}

		actions = append(actions, action)
		if action.Name() == "backfill" {
			actionOptions["backfill"] = map[string]string{
				conf.BackfillFlagName: confAction.Options[conf.BackfillFlagName],
			}
		}
	}

	return actions, actionOptions, nil
}

func readSchedulerConf(confPath string) (string, error) {
	dat, err := ioutil.ReadFile(confPath)
	if err != nil {
		return "", err
	}
	return string(dat), nil
}
