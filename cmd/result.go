package cmd

import (
	"bufio"
	"errors"
	"github.com/spf13/cobra"
	"github.com/tidwall/gjson"

	"github.com/olekukonko/tablewriter"
	"os"
	"strings"
)

// NewResultCommand return a config subcommand of rootCmd
func NewResultCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "result <subcommand>",
		Short: "deal result or report file",
	}
	cmd.AddCommand(NewParseCommand())
	return cmd
}

// NewParseCommand return a show subcommand of configCmd
func NewParseCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "parse <result or report file>",
		Short: "parse result or report file",
		Run:   parseResultFileCommandFunc,
	}
	return sc
}

func parseResultFileCommandFunc(cmd *cobra.Command, args []string) {

	if len(args) != 1 {
		cmd.PrintErrln(errors.New("Please input result or report file"))
		return
	}

	if !strings.HasSuffix(args[0], ".result") && !strings.HasSuffix(args[0], ".rep") {
		cmd.PrintErrln(errors.New("File must has suffix '.result' or '.rep'"))
		return
	}

	firestline := true
	fi, err := os.Open(args[0])
	if err != nil {
		cmd.PrintErrln(err)
		return
	}
	defer fi.Close()

	metadata := [][]string{}
	data := [][]string{}
	scanner := bufio.NewScanner(fi)
	for scanner.Scan() {
		fileline := scanner.Text()
		if firestline && strings.HasSuffix(args[0], ".rep") {
			metaarray := gjson.Parse(fileline).Array()

			for _, v := range metaarray {
				line := []string{
					gjson.Get(v.String(), "Source").String(),
					gjson.Get(v.String(), "Target").String(),
					gjson.Get(v.String(), "SourceDB").String(),
					gjson.Get(v.String(), "TargetDB").String(),
					gjson.Get(v.String(), "BatchSize").String(),
					gjson.Get(v.String(), "CompareThreads").String(),
					gjson.Get(v.String(), "KeyDiffReason").String(),
				}
				metadata = append(metadata, line)
			}
			firestline = false
			continue
		}

		source := ""
		target := ""
		sarray := gjson.Get(fileline, "Source").Array()
		tarray := gjson.Get(fileline, "Target").Array()

		for k, v := range sarray {
			if k == len(sarray)-1 {
				source = source + v.String()
			} else {
				source = source + v.String() + "\n"
			}

		}

		for k, v := range tarray {
			if k == len(tarray)-1 {
				target = target + v.String()
			} else {
				target = target + v.String() + "\n"
			}
		}

		line := []string{
			source,
			target,
			gjson.Get(fileline, "Key").String(),
			gjson.Get(fileline, "SourceDB").String(),
			gjson.Get(fileline, "TargetDB").String(),
			gjson.Get(fileline, "KeyDiffReason").String(),
		}

		data = append(data, line)
	}

	if err := scanner.Err(); err != nil {
		cmd.PrintErrln(err)
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetColWidth(12)
	table.SetHeader([]string{"Source", "Target", "Key", "SourceDB", "TargetDB", "KeyDiffReason"})
	//table.SetBorder(false)
	table.AppendBulk(data)
	table.Render()

}
