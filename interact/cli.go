package interact

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/chzyer/readline"
	"github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"rediscompare/check"
	"rediscompare/cmd"
	"rediscompare/commons"
	"io"
	"os"
	"strings"
)

type CommandFlags struct {
	URL      string
	CAPath   string
	CertPath string
	KeyPath  string
	Help     bool
}

var (
	commandFlags    = CommandFlags{}
	cfgFile         string
	detach          bool
	syncserver      string
	Confignotseterr error
	interact        bool
	version         bool
)

var LivePrefixState struct {
	LivePrefix string
	IsEnable   bool
}

var query = ""
var suggest = []prompt.Suggest{
	//config
	{Text: "config", Description: "config env"},
	{Text: "show ", Description: "show config"},
	{Text: "set ", Description: "set config"},
	{Text: "delete ", Description: "delete"},


	//task
	{Text: "task", Description: "about task"},
	{Text: "create ", Description: "create task"},
	{Text: "start ", Description: "start task"},
	{Text: "--afresh", Description: "start task afresh"},
	{Text: "remove ", Description: "remove task"},
	{Text: "stop ", Description: "stop task"},
	{Text: "status ", Description: "query task status"},
	{Text: "byname ", Description: "query task status by task name"},
	{Text: "bytaskid ", Description: "query task status by task id"},
	{Text: "bygroupid ", Description: "query task status by task group id"},
	{Text: "all ", Description: "query all tasks status "},
}

var readlinecompleter *readline.PrefixCompleter

func init() {
	cobra.EnablePrefixMatching = true
	cobra.OnInitialize(initConfig)

}

func cliRun(cmd *cobra.Command, args []string) {
	banner := "\n ___               _           ___                                               \n|  _`\\            ( ) _       (  _`\\                                             \n| (_) )   __     _| |(_)  ___ | ( (_)   _     ___ ___   _ _      _ _  _ __   __  \n| ,  /  /'__`\\ /'_` || |/',__)| |  _  /'_`\\ /' _ ` _ `\\( '_`\\  /'_` )( '__)/'__`\\\n| |\\ \\ (  ___/( (_| || |\\__, \\| (_( )( (_) )| ( ) ( ) || (_) )( (_| || |  (  ___/\n(_) (_)`\\____)`\\__,_)(_)(____/(____/'`\\___/'(_) (_) (_)| ,__/'`\\__,_)(_)  `\\____)\n                                                       | |                       \n                                                       (_)                       \n"

	if interact {
		err := check.CheckEnv()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		cmd.Println(banner)
		cmd.Println("Input 'help;' for usage. \nCommand must end with ';'. \n'tab' for command complete.\n^C or exit to quit.")
		loop()
		return
	}

	if len(args) == 0 {
		cmd.Help()
		return
	}

}

func getBasicCmd() *cobra.Command {

	rootCmd := &cobra.Command{
		Use:   "rediscompare",
		Short: "rediscompare command line interface",
		Long:  "",
	}

	rootCmd.PersistentFlags().BoolVarP(&commandFlags.Help, "help", "h", false, "help message")

	rootCmd.AddCommand(
		cmd.NewResultCommand(),
		cmd.NewCompareCommand(),
	)

	rootCmd.Flags().ParseErrorsWhitelist.UnknownFlags = true
	rootCmd.SilenceErrors = true
	return rootCmd
}

func getInteractCmd(args []string) *cobra.Command {
	rootCmd := getBasicCmd()
	rootCmd.Run = func(cmd *cobra.Command, args []string) {
	}

	rootCmd.SetArgs(args)
	rootCmd.ParseFlags(args)
	rootCmd.SetOut(os.Stdout)
	//rootCmd.SetOutput(os.Stdout)
	hiddenFlag(rootCmd)

	return rootCmd
}

func getMainCmd(args []string) *cobra.Command {
	rootCmd := getBasicCmd()

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.config.yaml)")
	rootCmd.PersistentFlags().StringVarP(&syncserver, "syncserver", "s", "", "sync server address")
	rootCmd.Flags().BoolVarP(&detach, "detach", "d", true, "Run pdctl without readline.")
	rootCmd.Flags().BoolVarP(&interact, "interact", "i", false, "Run pdctl with readline.")
	rootCmd.Flags().BoolVarP(&version, "version", "V", false, "Print version information and exit.")

	rootCmd.Run = cliRun

	rootCmd.SetArgs(args)
	rootCmd.ParseFlags(args)
	rootCmd.SetOut(os.Stdout)

	readlinecompleter = readline.NewPrefixCompleter(GenCompleter(rootCmd)...)
	return rootCmd
}

// Hide the flags in help and usage messages.
func hiddenFlag(cmd *cobra.Command) {
	cmd.LocalFlags().MarkHidden("pd")
	cmd.LocalFlags().MarkHidden("cacert")
	cmd.LocalFlags().MarkHidden("cert")
	cmd.LocalFlags().MarkHidden("key")
}

// MainStart start main command
func MainStart(args []string) {
	startCmd(getMainCmd, args)
}

// Start start interact command
func Start(args []string) {
	startCmd(getInteractCmd, args)
}

func startCmd(getCmd func([]string) *cobra.Command, args []string) {
	rootCmd := getCmd(args)

	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(err)
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {

	if syncserver == "" {
		fmt.Println(syncserver)
		syncserver = os.Getenv("SYNCSERVER")
	}

	if cfgFile != "" && commons.FileExists(cfgFile) {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName(".config")
	}

	viper.ReadInConfig()

	viper.AutomaticEnv() // read in environment variables that match

	if syncserver != "" {
		viper.Set("SYNCSERVER", syncserver)
	}

}

func loop() {
	rl, err := readline.NewEx(&readline.Config{
		//Prompt:            "\033[31m»\033[0m ",
		Prompt:                 "RedisCompare> ",
		HistoryFile:            "/tmp/readline.tmp",
		AutoComplete:           readlinecompleter,
		DisableAutoSaveHistory: true,
		InterruptPrompt:        "^C",
		EOFPrompt:              "^D",
		HistorySearchFold:      true,
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	var cmds []string

	for {
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		cmds = append(cmds, line)

		if !strings.HasSuffix(line, ";") {
			rl.SetPrompt("... ")
			continue
		}
		cmd := strings.Join(cmds, " ")
		cmds = cmds[:0]
		rl.SetPrompt("RedisCompare> ")
		rl.SaveHistory(cmd)

		args, err := shellwords.Parse(cmd)
		if err != nil {
			fmt.Printf("parse command err: %v\n", err)
			continue
		}
		Start(args)
	}
}

func GenCompleter(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	pc := []readline.PrefixCompleterInterface{}
	if len(cmd.Commands()) != 0 {
		for _, v := range cmd.Commands() {
			if v.HasFlags() {
				flagsPc := []readline.PrefixCompleterInterface{}
				flagUsages := strings.Split(strings.Trim(v.Flags().FlagUsages(), " "), "\n")
				for i := 0; i < len(flagUsages)-1; i++ {
					flagsPc = append(flagsPc, readline.PcItem(strings.Split(strings.Trim(flagUsages[i], " "), " ")[0]))
				}
				flagsPc = append(flagsPc, GenCompleter(v)...)
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], flagsPc...))

			} else {
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], GenCompleter(v)...))
			}
		}
	}
	return pc
}
