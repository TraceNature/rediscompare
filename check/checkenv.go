package check

import (
	"errors"
	"github.com/spf13/viper"
)

func CheckEnv() error {
	if viper.Get("syncserver") == "" {
		return errors.New("You must set syncserver from '-s' flag example 'redissyncer-cli -s http://yourip:port' or \n" +
			"use environment  variable example 'export SYNCSERVER=http://yourip:port' or \n edit .config.yaml append 'SYNCSERVER=http://yourip:port'")
	}
	return nil

}
