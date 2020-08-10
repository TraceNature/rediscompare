package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/ghodss/yaml"
	"io/ioutil"
	"os"
	"path/filepath"
	"rediscompare/commons"
	"rediscompare/compare"
	"rediscompare/globalzap"
	"strconv"
	"strings"
	"time"
)

var zaplogger = globalzap.GetLogger()

const (
	Scenario_single2single       = "single2single"
	Scenario_single2cluster      = "single2cluster"
	Scenario_multisingle2single  = "multisingle2single"
	Scenario_multisingle2cluster = "multisingle2cluster"
	Scenario_cluster2cluster     = "cluster2cluster"
)

type SAddr struct {
	Addr     string
	Password string
	Dbs      []int
}
type RedisCompare struct {
	Saddr        []SAddr `json:"saddr"`
	Taddr        string  `json:"taddr"`
	Spassword    string  `json:"spassword"`
	Tpassword    string  `json:"tpassword"`
	Sdb          int     `json:"sdb"`
	Tdb          int     `json:"tdb"`
	BatchSize    int     `json:"batchsize"`
	Threads      int     `json:"threads"`
	TTLdiff      int     `json:"ttldiff"`
	CompareTimes int     `json:"comparetimes"`
	Report       bool    `json:"report"`
	Scenario     string  `json:"scenario"`
}

func NewCompareCommand() *cobra.Command {
	compare := &cobra.Command{
		Use:   "compare <subcommand>",
		Short: "compare redis db",
	}

	compare.AddCommand(NewExecuteCommand())
	compare.AddCommand(NewSingle2SingleCommand())
	compare.AddCommand(NewSingle2ClusterCommand())
	compare.AddCommand(NewCluster2ClusterCommand())
	compare.AddCommand(NewMultiSingle2SingleCommand())
	//compare.AddCommand(NewMultiSingle2ClusterCommand())
	return compare
}

func NewExecuteCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "exec ",
		Short: "compare single instance redis",
		Run:   executeCommandFunc,
	}
	return sc
}
func NewSingle2SingleCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "single2single ",
		Short: "compare single instance redis",
		Run:   single2singleCommandFunc,
	}
	//sc.AddCommand(NewTaskCreateSourceCommand())
	//sc.Flags().Bool("afresh", false, "afresh task from begin")
	sc.Flags().String("saddr", "127.0.0.1:6379", "Source redis address default is 127.0.0.1:6379")
	sc.Flags().String("taddr", "127.0.0.1:6379", "Target redis address default is 127.0.0.1:6379")
	sc.Flags().String("spassword", "", "Source redis password")
	sc.Flags().String("tpassword", "", "Target redis password")
	sc.Flags().Int("sdb", 0, "Source redis DB number default is 0")
	sc.Flags().Int("tdb", 0, "Source redis DB number default is 0")
	sc.Flags().Int("batchsize", 50, "Compare List、Set、Zset type batch default is 50")
	sc.Flags().Int("threads", 0, "Compare threads default is cpu core number")
	sc.Flags().Int("ttldiff", 10000, "Diffrent of TTL,Allowed max ttl microseconds default is 10000 as ten seconds")
	sc.Flags().Int("comparetimes", 1, "compare loop times,default is 1")
	sc.Flags().Bool("report", false, "whether generate report default is false")
	return sc

}

func NewSingle2ClusterCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "single2cluster ",
		Short: "compare single instance redis and cluster data",
		Run:   single2clusterCommandFunc,
	}
	//sc.AddCommand(NewTaskCreateSourceCommand())
	sc.Flags().String("saddr", "127.0.0.1:6379", "Source redis address default is 127.0.0.1:6379")
	sc.Flags().String("taddr", "127.0.0.1:6379", "Target redis cluster addresses splite with ',' default is 127.0.0.1:6379")
	sc.Flags().String("spassword", "", "Source redis password")
	sc.Flags().String("tpassword", "", "Target redis password")
	sc.Flags().Int("sdb", 0, "Source redis DB number default is 0")
	//sc.Flags().Int("tdb", 0, "Source redis DB number default is 0")
	sc.Flags().Int("batchsize", 50, "Compare List、Set、Zset type batch default is 50")
	sc.Flags().Int("threads", 0, "Compare threads default is cpu core number")
	sc.Flags().Int("ttldiff", 10000, "Diffrent of TTL,Allowed max ttl microseconds default is 10000 as ten seconds")
	sc.Flags().Int("comparetimes", 1, "compare loop times,default is 1")
	sc.Flags().Bool("report", false, "whether generate report default is false")
	return sc

}

func NewMultiSingle2SingleCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "multisingle2single ",
		Short: "compare single instance redis and cluster data",
		Run:   multisingle2singleCommandFunc,
	}
	sc.Flags().String("saddr", "127.0.0.1:6379", "Source redis address default is 127.0.0.1:6379,multi address splite by ','")
	sc.Flags().String("taddr", "127.0.0.1:6379", "Target redis  addresses default is 127.0.0.1:6379")
	sc.Flags().String("spassword", "", "Source redis password")
	sc.Flags().String("tpassword", "", "Target redis password")
	sc.Flags().Int("sdb", 0, "Source redis DB number default is 0")
	sc.Flags().Int("tdb", 0, "Source redis DB number default is 0")
	sc.Flags().Int("batchsize", 50, "Compare List、Set、Zset type batch default is 50")
	sc.Flags().Int("threads", 0, "Compare threads default is cpu core number")
	sc.Flags().Int("ttldiff", 10000, "Diffrent of TTL,Allowed max ttl microseconds default is 10000 as ten seconds")
	sc.Flags().Int("comparetimes", 1, "compare loop times,default is 1")
	sc.Flags().Bool("report", false, "whether generate report default is false")
	return sc
}

func NewCluster2ClusterCommand() *cobra.Command {
	sc := &cobra.Command{
		Use:   "cluster2cluster <task description>",
		Short: "compare single instance redis",
		Run:   cluster2clusterCommandFunc,
	}
	//sc.AddCommand(NewTaskCreateSourceCommand())

	sc.Flags().String("saddr", "127.0.0.1:6379", "Source redis address default is 127.0.0.1:6379,multi address splite by ','")
	sc.Flags().String("taddr", "127.0.0.1:6379", "Target redis  addresses default is 127.0.0.1:6379")
	sc.Flags().String("spassword", "", "Source redis password")
	sc.Flags().String("tpassword", "", "Target redis password")
	//sc.Flags().Int("sdb", 0, "Source redis DB number default is 0")
	//sc.Flags().Int("tdb", 0, "Source redis DB number default is 0")
	sc.Flags().Int("batchsize", 50, "Compare List、Set、Zset type batch default is 50")
	sc.Flags().Int("threads", 0, "Compare threads default is cpu core number")
	sc.Flags().Int("ttldiff", 10000, "Diffrent of TTL,Allowed max ttl microseconds default is 10000 as ten seconds")
	sc.Flags().Int("comparetimes", 1, "compare loop times,default is 1")
	sc.Flags().Bool("report", false, "whether generate report default is false")
	return sc

}

func executeCommandFunc(cmd *cobra.Command, args []string) {
	//v := viper.New()
	//v.SetConfigType("yaml") // 设置配置文件的类型
	//v.SetConfigFile("./execyaml/multisingle2single.yml")
	//v.ReadInConfig()
	//
	//cmd.Println(v.Get(`Saddr`))

	if len(args) != 1 {
		cmd.PrintErrln(errors.New("Must input execute file path"))
		return
	}

	ymlbytes, err := ioutil.ReadFile(args[0])
	if err != nil {
		cmd.PrintErrln(err)
		return
	}

	jsonbytes, err := yaml.YAMLToJSON(ymlbytes)
	if err != nil {
		cmd.PrintErrln(err)
		return
	}
	var rc RedisCompare

	json.Unmarshal(jsonbytes, &rc)

	execerr := rc.Execute()
	if execerr != nil {
		cmd.PrintErrln(execerr)
	}

}

func single2singleCommandFunc(cmd *cobra.Command, args []string) {
	saddr, _ := cmd.Flags().GetString("saddr")
	taddr, _ := cmd.Flags().GetString("taddr")
	spassword, _ := cmd.Flags().GetString("spassword")
	tpassword, _ := cmd.Flags().GetString("tpassword")
	sdb, _ := cmd.Flags().GetInt("sdb")
	tdb, _ := cmd.Flags().GetInt("tdb")
	batchsize, _ := cmd.Flags().GetInt("batchsize")
	threas, _ := cmd.Flags().GetInt("threads")
	ttldiff, _ := cmd.Flags().GetInt("ttldiff")
	comparetimes, _ := cmd.Flags().GetInt("comparetimes")
	report, _ := cmd.Flags().GetBool("report")

	saddrstruct := SAddr{
		Addr:     saddr,
		Password: spassword,
		Dbs:      []int{sdb},
	}

	rc := RedisCompare{
		Saddr:        []SAddr{saddrstruct},
		Taddr:        taddr,
		Spassword:    spassword,
		Tpassword:    tpassword,
		Sdb:          sdb,
		Tdb:          tdb,
		BatchSize:    batchsize,
		Threads:      threas,
		TTLdiff:      ttldiff,
		CompareTimes: comparetimes,
		Report:       report,
		Scenario:     Scenario_single2single,
	}
	err := rc.Single2Single()
	if err != nil {
		cmd.PrintErrln(err)
	}
}

func multisingle2singleCommandFunc(cmd *cobra.Command, args []string) {

	saddr, _ := cmd.Flags().GetString("saddr")
	taddr, _ := cmd.Flags().GetString("taddr")
	spassword, _ := cmd.Flags().GetString("spassword")
	tpassword, _ := cmd.Flags().GetString("tpassword")
	sdb, _ := cmd.Flags().GetInt("sdb")
	tdb, _ := cmd.Flags().GetInt("tdb")
	batchsize, _ := cmd.Flags().GetInt("batchsize")
	threas, _ := cmd.Flags().GetInt("threads")
	ttldiff, _ := cmd.Flags().GetInt("ttldiff")
	comparetimes, _ := cmd.Flags().GetInt("comparetimes")
	report, _ := cmd.Flags().GetBool("report")

	saddrstruct := SAddr{
		Addr:     saddr,
		Password: spassword,
		Dbs:      []int{sdb},
	}

	rc := RedisCompare{
		Saddr:        []SAddr{saddrstruct},
		Taddr:        taddr,
		Spassword:    spassword,
		Tpassword:    tpassword,
		Sdb:          sdb,
		Tdb:          tdb,
		BatchSize:    batchsize,
		Threads:      threas,
		TTLdiff:      ttldiff,
		CompareTimes: comparetimes,
		Report:       report,
		Scenario:     Scenario_multisingle2single,
	}
	err := rc.MultiSingle2Single()

	if err != nil {
		cmd.Println(err)
	}
}

func single2clusterCommandFunc(cmd *cobra.Command, args []string) {
	saddr, _ := cmd.Flags().GetString("saddr")
	taddr, _ := cmd.Flags().GetString("taddr")
	spassword, _ := cmd.Flags().GetString("spassword")
	tpassword, _ := cmd.Flags().GetString("tpassword")
	sdb, _ := cmd.Flags().GetInt("sdb")
	//tdb, _ := cmd.Flags().GetInt("tdb")
	batchsize, _ := cmd.Flags().GetInt("batchsize")
	threas, _ := cmd.Flags().GetInt("threads")
	ttldiff, _ := cmd.Flags().GetInt("ttldiff")
	comparetimes, _ := cmd.Flags().GetInt("comparetimes")
	report, _ := cmd.Flags().GetBool("report")

	saddrstruct := SAddr{
		Addr:     saddr,
		Password: spassword,
		Dbs:      []int{sdb},
	}

	rc := RedisCompare{
		Saddr:     []SAddr{saddrstruct},
		Taddr:     taddr,
		Spassword: spassword,
		Tpassword: tpassword,
		Sdb:       sdb,
		//Tdb:          tdb,
		BatchSize:    batchsize,
		Threads:      threas,
		TTLdiff:      ttldiff,
		CompareTimes: comparetimes,
		Report:       report,
		Scenario:     Scenario_single2cluster,
	}

	err := rc.Single2Cluster()
	if err != nil {
		cmd.Println(err)
	}
}

func cluster2clusterCommandFunc(cmd *cobra.Command, args []string) {
	saddr, _ := cmd.Flags().GetString("saddr")
	taddr, _ := cmd.Flags().GetString("taddr")
	spassword, _ := cmd.Flags().GetString("spassword")
	tpassword, _ := cmd.Flags().GetString("tpassword")
	//sdb, _ := cmd.Flags().GetInt("sdb")
	//tdb, _ := cmd.Flags().GetInt("tdb")
	batchsize, _ := cmd.Flags().GetInt("batchsize")
	threas, _ := cmd.Flags().GetInt("threads")
	ttldiff, _ := cmd.Flags().GetInt("ttldiff")
	comparetimes, _ := cmd.Flags().GetInt("comparetimes")
	report, _ := cmd.Flags().GetBool("report")

	saddrs := strings.Split(saddr, ",")
	saddrstructs := []SAddr{}

	for _, v := range saddrs {
		saddr := SAddr{
			Addr:     v,
			Password: spassword,
			//Dbs:      []int{sdb},
		}
		saddrstructs = append(saddrstructs, saddr)
	}

	rc := RedisCompare{
		Saddr:     saddrstructs,
		Taddr:     taddr,
		Spassword: spassword,
		Tpassword: tpassword,
		//Sdb:       sdb,
		//Tdb:          tdb,
		BatchSize:    batchsize,
		Threads:      threas,
		TTLdiff:      ttldiff,
		CompareTimes: comparetimes,
		Report:       report,
		Scenario:     Scenario_cluster2cluster,
	}
	execerr := rc.Cluster2Cluster()
	if execerr != nil {
		cmd.PrintErrln(execerr)
	}
}

func (rc *RedisCompare) Execute() error {
	switch rc.Scenario {
	case Scenario_single2single:
		return rc.Single2Single()
	case Scenario_single2cluster:
		return rc.Single2Cluster()
	case Scenario_cluster2cluster:
		return rc.Cluster2Cluster()
	case Scenario_multisingle2single:
		return rc.MultiSingle2Single()
	case Scenario_multisingle2cluster:
		return rc.MultiSingle2Cluster()
	default:
		return errors.New("Scenario not exists")
	}
	return nil
}

func (rc *RedisCompare) Single2Single() error {

	if len(rc.Saddr) == 0 {
		return errors.New("No saddrs")
	}

	if rc.CompareTimes < 1 {
		rc.CompareTimes = 1
	}
	saddr := rc.Saddr[0]

	sopt := &redis.Options{
		Addr: saddr.Addr,
		DB:   saddr.Dbs[0],
	}

	if saddr.Password != "" {
		sopt.Password = saddr.Password
	}
	sclient := commons.GetGoRedisClient(sopt)

	topt := &redis.Options{
		Addr: rc.Taddr,
		DB:   rc.Tdb,
	}

	if rc.Tpassword != "" {
		topt.Password = rc.Tpassword
	}

	tclient := commons.GetGoRedisClient(topt)

	defer sclient.Close()
	defer tclient.Close()

	//check redis 连通性
	if !commons.CheckRedisClientConnect(sclient) {
		return errors.New("Cannot connect source redis")
	}
	if !commons.CheckRedisClientConnect(tclient) {
		return errors.New("Cannot connect source redis")
	}

	//删除目录下上次运行时临时产生的result文件
	files, _ := filepath.Glob("*.result")
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	compare := &compare.CompareSingle2Single{
		Source:         sclient,
		Target:         tclient,
		BatchSize:      int64(rc.BatchSize),
		TTLDiff:        float64(rc.TTLdiff),
		RecordResult:   true,
		CompareThreads: rc.Threads,
	}
	var compares []interface{}
	compare.CompareDB()

	for i := 0; i < rc.CompareTimes-1; i++ {
		compare.CompareKeysFromResultFile([]string{compare.ResultFile})
	}

	comparemap, _ := commons.Struct2Map(compare)
	comparemap["Source"] = compare.Source.Options().Addr
	comparemap["Target"] = compare.Target.Options().Addr
	compares = append(compares, comparemap)

	//生成报告
	if rc.Report {
		GenReport([]string{compare.ResultFile}, compares)

	}
	return nil
}

func (rc *RedisCompare) Single2Cluster() error {

	if len(rc.Saddr) == 0 {
		return errors.New("No saddrs")
	}

	if rc.CompareTimes < 1 {
		rc.CompareTimes = 1
	}

	saddr := rc.Saddr[0]

	sopt := &redis.Options{
		Addr: saddr.Addr,
		DB:   saddr.Dbs[0],
	}

	if saddr.Password != "" {
		sopt.Password = saddr.Password
	}
	sclient := commons.GetGoRedisClient(sopt)

	topt := &redis.ClusterOptions{
		Addrs: strings.Split(rc.Taddr, ","),
	}

	if rc.Tpassword != "" {
		topt.Password = rc.Tpassword
	}

	tclient := redis.NewClusterClient(topt)

	defer sclient.Close()
	defer tclient.Close()

	//check redis 连通性
	if !commons.CheckRedisClientConnect(sclient) {
		return errors.New("Cannot connect source redis")

	}
	if !commons.CheckRedisClusterClientConnect(tclient) {
		return errors.New("Cannot connect source redis")
	}

	//删除目录下上次运行时临时产生的result文件
	files, _ := filepath.Glob("*.result")
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	compare := &compare.CompareSingle2Cluster{
		Source:         sclient,
		Target:         tclient,
		BatchSize:      int64(rc.BatchSize),
		TTLDiff:        float64(rc.TTLdiff),
		RecordResult:   true,
		CompareThreads: rc.Threads,
	}

	var compares []interface{}

	compare.CompareDB()

	for i := 0; i < rc.CompareTimes-1; i++ {
		compare.CompareKeysFromResultFile([]string{compare.ResultFile})
	}
	comparemap, _ := commons.Struct2Map(compare)
	comparemap["Source"] = compare.Source.Options().Addr
	comparemap["Target"] = compare.Target.Options().Addrs
	compares = append(compares, comparemap)

	//生成报告
	if rc.Report {
		GenReport([]string{compare.ResultFile}, compares)

	}
	return nil
}

func (rc *RedisCompare) MultiSingle2Single() error {

	if len(rc.Saddr) == 0 {
		return errors.New("No source address")
	}

	var sclients []*redis.Client

	if rc.CompareTimes < 1 {
		rc.CompareTimes = 1
	}

	for _, v := range rc.Saddr {
		if len(v.Dbs) == 0 {
			continue
		}
		for _, vdb := range v.Dbs {
			sopt := &redis.Options{
				Addr: v.Addr,
				DB:   vdb,
			}
			if v.Password != "" {
				sopt.Password = v.Password
			}
			sclient := commons.GetGoRedisClient(sopt)
			sclients = append(sclients, sclient)
		}

	}

	topt := &redis.Options{
		Addr: rc.Taddr,
		DB:   rc.Tdb,
	}

	if rc.Tpassword != "" {
		topt.Password = rc.Tpassword
	}

	tclient := commons.GetGoRedisClient(topt)

	defer tclient.Close()

	for _, v := range sclients {
		//check redis 连通性
		if !commons.CheckRedisClientConnect(v) {
			return errors.New("Cannot connect source redis: " + v.Options().Addr + "|" + strconv.Itoa(v.Options().DB))
		}
	}

	if !commons.CheckRedisClientConnect(tclient) {
		return errors.New("Cannot connect source redis")
	}

	//删除目录下上次运行时临时产生的result文件
	files, _ := filepath.Glob("*.result")
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	resultfiles := []string{}
	var compares []interface{}
	for _, v := range sclients {
		compare := &compare.CompareSingle2Single{
			Source:         v,
			Target:         tclient,
			BatchSize:      int64(rc.BatchSize),
			TTLDiff:        float64(rc.TTLdiff),
			RecordResult:   true,
			CompareThreads: rc.Threads,
		}

		compare.CompareDB()

		for i := 0; i < rc.CompareTimes-1; i++ {
			compare.CompareKeysFromResultFile([]string{compare.ResultFile})
		}
		resultfiles = append(resultfiles, compare.ResultFile)
		comparemap, _ := commons.Struct2Map(compare)
		comparemap["Source"] = compare.Source.Options().Addr
		comparemap["Target"] = compare.Target.Options().Addr
		compares = append(compares, comparemap)

	}

	//生成报告
	if rc.Report {
		GenReport(resultfiles, compares)
	}
	for _, v := range sclients {
		v.Close()
	}

	return nil
}

func (rc *RedisCompare) MultiSingle2Cluster() error {

	if len(rc.Saddr) == 0 {
		return errors.New("No source address")
	}

	var sclients []*redis.Client

	if rc.CompareTimes < 1 {
		rc.CompareTimes = 1
	}

	for _, v := range rc.Saddr {
		if len(v.Dbs) == 0 {
			continue
		}
		for _, vdb := range v.Dbs {
			sopt := &redis.Options{
				Addr: v.Addr,
				DB:   vdb,
			}
			if v.Password != "" {
				sopt.Password = v.Password
			}
			sclient := commons.GetGoRedisClient(sopt)
			sclients = append(sclients, sclient)
		}
	}

	topt := &redis.ClusterOptions{
		Addrs: strings.Split(rc.Taddr, ","),
	}

	if rc.Tpassword != "" {
		topt.Password = rc.Tpassword
	}

	tclient := redis.NewClusterClient(topt)
	defer tclient.Close()

	for _, v := range sclients {
		//check redis 连通性
		if !commons.CheckRedisClientConnect(v) {
			return errors.New("Cannot connect source redis")
		}
	}

	if !commons.CheckRedisClusterClientConnect(tclient) {
		//cmd.PrintErrln(errors.New("Cannot connect target redis"))
		return errors.New("Cannot connect source redis")
	}

	//删除目录下上次运行时临时产生的result文件
	files, _ := filepath.Glob("*.result")
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	resultfiles := []string{}
	//compares := []*compare.CompareSingle2Cluster{}
	var compares []interface{}
	for _, v := range sclients {
		compare := &compare.CompareSingle2Cluster{
			Source:         v,
			Target:         tclient,
			BatchSize:      int64(rc.BatchSize),
			TTLDiff:        float64(rc.TTLdiff),
			RecordResult:   true,
			CompareThreads: rc.Threads,
		}

		compare.CompareDB()

		for i := 0; i < rc.CompareTimes-1; i++ {
			compare.CompareKeysFromResultFile([]string{compare.ResultFile})
		}
		resultfiles = append(resultfiles, compare.ResultFile)
		comparemap, _ := commons.Struct2Map(compare)
		comparemap["Source"] = compare.Source.Options().Addr
		comparemap["Target"] = compare.Target.Options().Addrs
		compares = append(compares, comparemap)

	}

	//生成报告
	if rc.Report {
		GenReport(resultfiles, compares)

	}
	for _, v := range sclients {
		v.Close()
	}

	return nil
}

func (rc *RedisCompare) Cluster2Cluster() error {

	if len(rc.Saddr) == 0 {
		return errors.New("No source address")
	}

	var sclients []*redis.Client

	if rc.CompareTimes < 1 {
		rc.CompareTimes = 1
	}

	for _, v := range rc.Saddr {
		sopt := &redis.Options{
			Addr: v.Addr,
			DB:   0,
		}
		if rc.Spassword != "" {
			sopt.Password = rc.Spassword
		}
		sclient := commons.GetGoRedisClient(sopt)
		sclients = append(sclients, sclient)
	}

	topt := &redis.ClusterOptions{
		Addrs: strings.Split(rc.Taddr, ","),
	}

	if rc.Tpassword != "" {
		topt.Password = rc.Tpassword
	}

	tclient := redis.NewClusterClient(topt)
	defer tclient.Close()

	for _, v := range sclients {
		//check redis 连通性
		if !commons.CheckRedisClientConnect(v) {
			return errors.New("Cannot connect source redis")
		}
	}

	if !commons.CheckRedisClusterClientConnect(tclient) {
		return errors.New("Cannot connect source redis")
	}

	//删除目录下上次运行时临时产生的result文件
	files, _ := filepath.Glob("*.result")
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	resultfiles := []string{}
	//compares := []*compare.CompareSingle2Cluster{}
	var compares []interface{}
	for _, v := range sclients {
		compare := &compare.CompareSingle2Cluster{
			Source:         v,
			Target:         tclient,
			BatchSize:      int64(rc.BatchSize),
			TTLDiff:        float64(rc.TTLdiff),
			RecordResult:   true,
			CompareThreads: rc.Threads,
		}
		compare.CompareDB()
		for i := 0; i < rc.CompareTimes-1; i++ {
			compare.CompareKeysFromResultFile([]string{compare.ResultFile})
		}
		resultfiles = append(resultfiles, compare.ResultFile)

		comparemap, _ := commons.Struct2Map(compare)
		comparemap["Source"] = compare.Source.Options().Addr
		comparemap["Target"] = compare.Target.Options().Addrs
		compares = append(compares, comparemap)

	}

	//生成报告
	if rc.Report {
		GenReport(resultfiles, compares)
	}

	for _, v := range sclients {
		v.Close()
	}
	return nil
}

func GenReport(resultfiles []string, compares []interface{}) error {
	reportfile := "./compare_" + time.Now().Format("20060102150405") + ".rep"

	jsonBytes, _ := json.Marshal(compares)
	commons.AppendLineToFile(bytes.NewBuffer(jsonBytes), reportfile)
	for _, v := range resultfiles {
		fi, err := os.Open(v)
		if err != nil {
			return err
		}
		defer fi.Close()
		scanner := bufio.NewScanner(fi)
		for scanner.Scan() {
			line := scanner.Text()
			commons.AppendLineToFile(bytes.NewBuffer([]byte(line)), reportfile)
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}
