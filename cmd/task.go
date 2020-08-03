package cmd

import (
	"encoding/json"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
	"interactioncli/commons"
	"interactioncli/httpquerry"
	"io/ioutil"
	"os"
	"strings"
	"github.com/olekukonko/tablewriter"
)

func NewTaskCommand() *cobra.Command {
	task := &cobra.Command{
		Use:   "task <subcommand>",
		Short: "operate task",
	}
	task.AddCommand(NewTaskCreateCommand())
	task.AddCommand(NewTaskStartCommand())
	task.AddCommand(NewTaskStopCommand())
	task.AddCommand(NewTaskRemoveCommand())
	task.AddCommand(NewTaskStatusCommand())
	return task
}

func NewTaskCreateCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "create <task description>",
		Short: "create task",
		Run:   createTaskCommandFunc,
	}
	sc.AddCommand(NewTaskCreateSourceCommand())
	return sc
}

func NewTaskCreateSourceCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "source <task description file path>",
		Short: "create task from file",
		Run:   createTaskSourceCommandFunc,
	}

	return sc
}

func NewTaskStartCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "start <taskid>",
		Short: "start task",
		Run:   startTaskCommandFunc,
	}

	sc.Flags().Bool("afresh", false, "afresh task from begin")
	sc.Flags().Bool("bfresh", false, "")
	return sc
}

//func NewTaskStartAfreshCommand() *cobra.Command {
//	sc := &cobra.Command{
//		Use:   "afresh",
//		Short: "start task afresh",
//		Run:   startTaskCommandFunc,
//	}
//	return sc
//}

func NewTaskStopCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "stop [taskid1 taskid2 ...]",
		Short: "stop task",
		Run:   stopTaskCommandFunc,
	}

	return sc
}

func NewTaskRemoveCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "remove <taskid>",
		Short: "remove task",
		Run:   removeTaskCommandFunc,
	}
	return sc
}

func NewTaskStatusCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "status [byname|bytaskid|bygroupid|all]",
		Short: "query task status",
	}
	sc.AddCommand(NewTaskStatusAllCommand())
	sc.AddCommand(NewTaskStatusBynameCommand())
	sc.AddCommand(NewTaskStatusBytaskidCommand())
	sc.AddCommand(NewTaskStatusBygroupidCommand())

	return sc
}

func NewTaskStatusAllCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "all",
		Short: "query all task status ",
		Run:   taskStatusAllCommandFunc,
	}
	return sc
}

func NewTaskStatusBynameCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "byname <taskname>",
		Short: "query task status by taskname",
		Run:   taskStatusBynameCommandFunc,
	}
	return sc
}

func NewTaskStatusBytaskidCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "bytaskid <taskid>",
		Short: "query task status by taskid",
		Run:   taskStatusBytaskidCommandFunc,
	}
	return sc
}

func NewTaskStatusBygroupidCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "bygroupid <groupid>",
		Short: "query task status by groupid",
		Run:   taskStatusBygroupidCommandFunc,
	}
	return sc
}

func createTaskCommandFunc(cmd *cobra.Command, args []string) {
	//if len(args) != 1 {
	//	cmd.PrintErrln("Must specific create task json or jsonfile")
	//	return
	//}
	cmdpath := strings.Split(cmd.CommandPath(), " ")

	if cmdpath[len(cmdpath)-1] == "source" {
		return
	}
	for _, v := range args {
		//jsonmap := make(map[string]interface{})
		//jsonmap["taskid"] = args[0]
		//
		//createjsonStr, err := json.Marshal(jsonmap)
		//if err != nil {
		//	cmd.PrintErr(err)
		//	return
		//}
		createreq := &httpquerry.Request{
			Server: viper.GetString("syncserver"),
			Api:    httpquerry.CreateTaskPath,
			Body:   v,
		}

		createresp, err := createreq.ExecRequest()
		if err != nil {
			cmd.PrintErr(err)
			return
		}

		cmd.Println(createresp)
	}

	//cmd.Println(args[0])
	//cmd.Println(viper.Get("syncserver"))
	//viper.Set("a", time.Now())
}
func createTaskSourceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		cmd.PrintErrln("Please input create json file path")
		return
	}

	for _, v := range args {
		if !commons.FileExists(v) {

			cmd.PrintErrf("file %s not exists \n", v)
			continue

		}
		jsonFile, err := os.Open(v)
		defer jsonFile.Close()
		if err != nil {
			cmd.PrintErrln(err)
			continue
		}

		json, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			cmd.PrintErrln(err)
			continue
		}

		createreq := &httpquerry.Request{
			Server: viper.GetString("syncserver"),
			Api:    httpquerry.CreateTaskPath,
			Body:   string(json),
		}

		createresp, err := createreq.ExecRequest()
		if err != nil {
			cmd.PrintErr(err)
			return
		}

		cmd.Println(createresp)
	}
}
func startTaskCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.PrintErrln("must input task id'")
		return
	}

	jsonmap := make(map[string]interface{})

	afresh, err := cmd.Flags().GetBool("afresh")
	if err != nil {
		cmd.Println(err)
		return
	}

	if afresh {
		jsonmap["afresh"] = true
	}

	jsonmap["taskid"] = args[0]

	startjsonStr, err := json.Marshal(jsonmap)
	if err != nil {
		cmd.PrintErr(err)
		return
	}
	startreq := &httpquerry.Request{
		Server: viper.GetString("syncserver"),
		Api:    httpquerry.StartTaskPath,
		Body:   string(startjsonStr),
	}

	listresp, err := startreq.ExecRequest()
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	cmd.Println(listresp)
}

func stopTaskCommandFunc(cmd *cobra.Command, args []string) {
	jsonmap := make(map[string]interface{})
	jsonmap["taskids"] = args

	stopjsonStr, err := json.Marshal(jsonmap)
	if err != nil {
		cmd.PrintErr(err)
		return
	}
	stopreq := &httpquerry.Request{
		Server: viper.GetString("syncserver"),
		Api:    httpquerry.StopTaskPath,
		Body:   string(stopjsonStr),
	}

	listresp, err := stopreq.ExecRequest()
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	cmd.Println(listresp)
}

func removeTaskCommandFunc(cmd *cobra.Command, args []string) {
	jsonmap := make(map[string]interface{})
	jsonmap["taskids"] = args

	removejsonStr, err := json.Marshal(jsonmap)
	if err != nil {
		cmd.PrintErr(err)
		return
	}
	removereq := &httpquerry.Request{
		Server: viper.GetString("syncserver"),
		Api:    httpquerry.RemoveTaskPath,
		Body:   string(removejsonStr),
	}

	listresp, err := removereq.ExecRequest()
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	cmd.Println(listresp)
}

func taskStatusAllCommandFunc(cmd *cobra.Command, args []string) {

	jsonmap := make(map[string]interface{})
	jsonmap["regulation"] = "all"

	listtaskjsonStr, err := json.Marshal(jsonmap)
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	listreq := &httpquerry.Request{
		Server: viper.GetString("syncserver"),
		Api:    httpquerry.ListTasksPath,
		Body:   string(listtaskjsonStr),
	}

	listresp, resperr := listreq.ExecRequest()

	if resperr != nil {
		cmd.PrintErr(resperr)
		return
	}

	data := [][]string{}
	taskarray := gjson.Get(listresp, "data").Array()

	for _, v := range taskarray {
		line := []string{
			gjson.Get(v.String(), "groupId").String(),
			gjson.Get(v.String(), "taskId").String(),
			gjson.Get(v.String(), "taskName").String(),
			gjson.Get(v.String(), "syncType").String(),
			gjson.Get(v.String(), "status").String(),
			gjson.Get(v.String(), "sourceRedisAddress").String(),
			//gjson.Get(v.String(), "targetRedisAddress").String(),
		}
		data = append(data, line)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetColWidth(18)
	table.SetHeader([]string{"groupId", "taskId", "taskName", "syncType", "status", "sourceRedisAddress"})
	//table.SetBorder(false)
	table.AppendBulk(data)
	table.Render()
	//cmd.Println(table)
	//cmd.Println(listresp)
}

func taskStatusBynameCommandFunc(cmd *cobra.Command, args []string) {
	jsonmap := make(map[string]interface{})
	jsonmap["regulation"] = "bynames"
	jsonmap["tasknames"] = args

	listtaskjsonStr, err := json.Marshal(jsonmap)
	if err != nil {
		cmd.PrintErr(err)
		return
	}
	listreq := &httpquerry.Request{
		Server: viper.GetString("syncserver"),
		Api:    httpquerry.ListTasksPath,
		Body:   string(listtaskjsonStr),
	}

	listresp, err := listreq.ExecRequest()
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	cmd.Println(listresp)
}

func taskStatusBytaskidCommandFunc(cmd *cobra.Command, args []string) {

	jsonmap := make(map[string]interface{})
	jsonmap["regulation"] = "byids"
	jsonmap["taskids"] = args

	listtaskjsonStr, err := json.Marshal(jsonmap)
	if err != nil {
		cmd.PrintErr(err)
		return
	}
	listreq := &httpquerry.Request{
		Server: viper.GetString("syncserver"),
		Api:    httpquerry.ListTasksPath,
		Body:   string(listtaskjsonStr),
	}

	listresp, err := listreq.ExecRequest()
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	cmd.Println(listresp)
}

func taskStatusBygroupidCommandFunc(cmd *cobra.Command, args []string) {
	cmd.Println("taskStatusBygroupidCommandFunc")

	jsonmap := make(map[string]interface{})
	jsonmap["regulation"] = "byGroupIds"
	jsonmap["groupIds"] = args

	listtaskjsonStr, err := json.Marshal(jsonmap)
	if err != nil {
		cmd.PrintErr(err)
		return
	}
	listreq := &httpquerry.Request{
		Server: viper.GetString("syncserver"),
		Api:    httpquerry.ListTasksPath,
		Body:   string(listtaskjsonStr),
	}

	listresp, err := listreq.ExecRequest()
	if err != nil {
		cmd.PrintErr(err)
		return
	}

	cmd.Println(listresp)

}
