package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"interactioncli/commons"
)

// NewConfigCommand return a config subcommand of rootCmd
func NewConfigCommand() *cobra.Command {
	conf := &cobra.Command{
		Use:   "config <subcommand>",
		Short: "tune pd configs",
	}
	conf.AddCommand(NewShowConfigCommand())
	//conf.AddCommand(NewSetConfigCommand())

	return conf
}

// NewShowConfigCommand return a show subcommand of configCmd
func NewShowConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "show [replication|label-property|all]",
		Short: "show replication and schedule config of PD",
		Run:   showConfigCommandFunc,
	}
	sc.AddCommand(NewShowAllConfigCommand())
	//sc.AddCommand(NewShowScheduleConfigCommand())
	//sc.AddCommand(NewShowReplicationConfigCommand())
	//sc.AddCommand(NewShowLabelPropertyCommand())

	return sc
}

// NewShowAllConfigCommand return a show all subcommand of show subcommand
func NewShowAllConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "all",
		Short: "show all config of redissyncer-cli",
		Run:   showAllConfigCommandFunc,
	}
	return sc
}

// NewShowScheduleConfigCommand return a show all subcommand of show subcommand
func NewShowScheduleConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "schedule",
		Short: "show schedule config of PD",
		Run:   showScheduleConfigCommandFunc,
	}
	return sc
}

// NewShowReplicationConfigCommand return a show all subcommand of show subcommand
func NewShowReplicationConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "replication",
		Short: "show replication config of PD",
		Run:   showReplicationConfigCommandFunc,
	}
	return sc
}

// NewShowLabelPropertyCommand returns a show label property subcommand of show subcommand.
func NewShowLabelPropertyCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "label-property",
		Short: "show label property config",
		Run:   showLabelPropertyConfigCommandFunc,
	}
	return sc
}

// NewSetConfigCommand return a set subcommand of configCmd
func NewSetConfigCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "set <option> <value>, set label-property <type> <key> <value>, set cluster-version <version>",
		Short: "set the option with value",
		Run:   setConfigCommandFunc,
	}
	sc.AddCommand(newSetReplicationModeCommand())
	return sc
}

func newSetReplicationModeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "replication-mode <mode> [<key>, <value>]",
		Short: "set replication mode config",
		Run:   setReplicationModeCommandFunc,
	}
}

func setReplicationModeCommandFunc(cmd *cobra.Command, args []string) {

	cmd.Println(cmd.Args)
}

func setConfigCommandFunc(cmd *cobra.Command, args []string) {

	cmd.Println(cmd.Args)
}

func showConfigCommandFunc(cmd *cobra.Command, args []string) {

	cmd.Println(cmd.Args)
}

func showScheduleConfigCommandFunc(cmd *cobra.Command, args []string) {
	cmd.Println(cmd.Args)
}

func showReplicationConfigCommandFunc(cmd *cobra.Command, args []string) {
	cmd.Println(cmd.Args)
}

func showLabelPropertyConfigCommandFunc(cmd *cobra.Command, args []string) {
	cmd.Println(cmd.Args)
}

func showAllConfigCommandFunc(cmd *cobra.Command, args []string) {
	configs, err := commons.MapToYamlString(viper.AllSettings())
	if err != nil {
		cmd.PrintErrln(err)
		return
	}
	cmd.Println(configs)
}
