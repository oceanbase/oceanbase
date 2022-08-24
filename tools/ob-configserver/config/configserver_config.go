/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

package config

import (
	"bytes"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v3"
)

type ConfigServerConfig struct {
	Log     *LogConfig     `yaml:"log"`
	Server  *ServerConfig  `yaml:"server"`
	Storage *StorageConfig `yaml:"storage"`
	Vip     *VipConfig     `yaml:"vip"`
}

func ParseConfigServerConfig(configFilePath string) (*ConfigServerConfig, error) {
	_, err := os.Stat(configFilePath)
	if err != nil {
		return nil, err
	}

	content, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}

	config := new(ConfigServerConfig)
	err = yaml.NewDecoder(bytes.NewReader(content)).Decode(config)
	if err != nil {
		return nil, err
	}
	return config, nil
}
