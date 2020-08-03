package cmd

import "github.com/spf13/cobra"

func NewCompareCommand() *cobra.Command {
	compare := &cobra.Command{
		Use:   "compare <subcommand>",
		Short: "compare redis db",
	}
	compare.AddCommand(NewTaskCreateCommand())
	return compare
}
