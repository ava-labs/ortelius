/*
Copyright © 2019 AVA Labs <collin@avalabs.org>
*/

package cfg

import (
	"github.com/ava-labs/ortelius/utils"
	"github.com/spf13/viper"
)

// Viper is a global instance of viper
var Viper *viper.Viper

// InitConfig initializes the config for commands to reference
func InitConfig(context string, file string) {
	Viper = viper.NewWithOptions(viper.KeyDelimiter("::"))
	Viper.SetDefault("context", context)
	viper.SetConfigType("json")
	Viper.SetConfigFile(file)
	if err := Viper.ReadInConfig(); err != nil {
		utils.Die("Can't read config: %s", err.Error())
	}
	Viper.Set("context", context)
}
