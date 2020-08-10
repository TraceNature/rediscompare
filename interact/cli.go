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
	//viper.Set("syncserver", syncserver)

	if interact {
		err := check.CheckEnv()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
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

	//for _, v := range rootCmd.Commands() {
	//	fmt.Println(v.Use)
	//}
	readlinecompleter = readline.NewPrefixCompleter(GenCompleter(rootCmd)...)

	//readlinecompleter = readline.NewPrefixCompleter(readline.PcItem("start", readline.PcItem("--abc")))
	//rc := readline.NewPrefixCompleter(GenCompleter(rootCmd)...)
	//for _, v := range rc.Children {
	//	fmt.Println(v.GetName())
	//}

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
	//if len(commandFlags.CAPath) != 0 {
	//	if err := command.InitHTTPSClient(commandFlags.CAPath, commandFlags.CertPath, commandFlags.KeyPath); err != nil {
	//		rootCmd.Println(err)
	//		return
	//	}
	//}

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

	// If a config file is found, read it in.
	//if err := viper.ReadInConfig(); err != nil {
	//	Confignotseterr = err
	//}
	viper.ReadInConfig()

	viper.AutomaticEnv() // read in environment variables that match

	if syncserver != "" {
		viper.Set("SYNCSERVER", syncserver)
	}

}

func loop() {
	rl, err := readline.NewEx(&readline.Config{
		//Prompt:            "\033[31m»\033[0m ",
		Prompt:                 "rediscompare-cli> ",
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
		rl.SetPrompt("rediscompare-cli> ")
		rl.SaveHistory(cmd)

		args, err := shellwords.Parse(cmd)
		if err != nil {
			fmt.Printf("parse command err: %v\n", err)
			continue
		}

		//args = append(args, "-u", commandFlags.URL)
		//if commandFlags.CAPath != "" && commandFlags.CertPath != "" && commandFlags.KeyPath != "" {
		//	args = append(args, "--cacert", commandFlags.CAPath, "--cert", commandFlags.CertPath, "--key", commandFlags.KeyPath)
		//}
		Start(args)
	}
}

func completer(in prompt.Document) []prompt.Suggest {
	w := in.GetWordBeforeCursor()

	if w == "" {
		return []prompt.Suggest{}
	}

	return prompt.FilterHasPrefix(suggest, w, true)
}

func executor(in string) {
	//fmt.Println(in)
	if strings.ToLower(in) == "exit" {
		os.Exit(0)
	}

	args, err := shellwords.Parse(in)
	if err != nil {
		fmt.Printf("parse command err: %v\n", err)
		return
	}
	Start(args)
}

func ListCommandTree(cmd *cobra.Command, level int) {
	if len(cmd.Commands()) != 0 {
		for _, v := range cmd.Commands() {
			for i := 0; i < level; i++ {
				fmt.Print("    ")
			}
			fmt.Println(strings.Split(v.Use, " ")[0])
			ListCommandTree(v, level+1)
		}
	}
}

func GenCompleter(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	pc := []readline.PrefixCompleterInterface{}
	if len(cmd.Commands()) != 0 {
		for _, v := range cmd.Commands() {
			//fmt.Println(strings.Split(v.Use, " ")[0]
			if v.HasFlags() {
				flagspc := []readline.PrefixCompleterInterface{}

				flaguseages := strings.Split(strings.Trim(v.Flags().FlagUsages(), " "), "\n")

				for i := 0; i < len(flaguseages)-1; i++ {
					flagspc = append(flagspc, readline.PcItem(strings.Split(strings.Trim(flaguseages[i], " "), " ")[0]))
				}
				flagspc = append(flagspc, GenCompleter(v)...)
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], flagspc...))

			} else {
				pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], GenCompleter(v)...))
			}
		}
	}
	return pc
}
