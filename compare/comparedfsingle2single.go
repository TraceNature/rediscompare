package compare

import (
	"bytes"
	"encoding/json"
	"rediscompare/commons"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"go.uber.org/zap"
)

type CompareDfSingle2Single struct {
	Source         []*redis.Client //connections to source
	Target         []*redis.Client //connections to target
	RecordResult   bool
	ResultFile     string
	BatchSize      int64 //Batch request in pipeline mode
	CompareThreads int   //比较db线程数量
	SourceDB       int   //redis DB number
	TargetDB       int   //redis DB number
}

func (compare *CompareDfSingle2Single) CompareDB() {
	resultfilestring := "./" + "compare_" + strconv.FormatInt(time.Now().UnixNano(), 10) + ".result"
	if compare.ResultFile == "" {
		compare.ResultFile = resultfilestring
	}

	zaplogger.Sugar().Info("CompareDfSingle2Single DB begin")

	// Compare the DB size of source and target db
	sourceSize, targetSize := compare.CompareDbSize()
	// Scan all keys in source and target and compare the scan result
	keys := compare.CompareKeys(sourceSize, targetSize)
	// Compare the values of keys which found both in source and target
	compare.CompareValues(keys)

	zaplogger.Sugar().Info("CompareDfSingle2Single End")
}

func (compare *CompareDfSingle2Single) CompareDbSize() (int64, int64) {
	zaplogger.Sugar().Info("Comparing DB size")
	srcClient := compare.Source[0]
	targetClient := compare.Target[0]
	compareresult := NewCompareResult()
	reason := make(map[string]interface{})
	compareresult.Source = srcClient.Options().Addr
	compareresult.Target = targetClient.Options().Addr
	compareresult.SourceDB = compare.SourceDB
	compareresult.TargetDB = compare.TargetDB

	sourcesize := srcClient.DbSize()
	targetsize := targetClient.DBSize()

	if sourcesize.Val() != targetsize.Val() {
		compareresult.IsEqual = false
		reason["description"] = "Source and Target db size different"
		reason["source"] = sourcesize.Val()
		reason["target"] = targetsize.Val()
		compareresult.KeyDiffReason = append(compareresult.KeyDiffReason, reason)

		zaplogger.Info("", zap.Any("CompareResult", compareresult))
		if compare.RecordResult {
			jsonBytes, _ := json.Marshal(compareresult)
			commons.AppendLineToFile(bytes.NewBuffer(jsonBytes), compare.ResultFile)
		}
	}
	zaplogger.Sugar().Info("Comparing DB size finished")
	return sourcesize.Val(), targetsize.Val()
}

func (compare *CompareDfSingle2Single) CompareKeys(sourceSize int64, targetSize int64) map[string]bool {
	zaplogger.Sugar().Info("Comparing DB Keys")

	sourceKeys := make(map[string]bool)
	targetKeys := make(map[string]bool)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		// Scan all source db keys and insert to sourceKeys
		compare.ScanKeysToMap(compare.Source[0], sourceSize, sourceKeys)
		wg.Done()
	}()

	go func() {
		// Scan all target db keys and insert to targetKeys
		compare.ScanKeysToMap(compare.Target[0], targetSize, targetKeys)
		wg.Done()
	}()
	wg.Wait()

	intersectionKeys := sourceKeys
	if !reflect.DeepEqual(sourceKeys, targetKeys) {
		// If maps are not equal remove the diff keys from source map to get
		// the keys which are both in source and target
		diffKeys := compare.FindKeysDiff(sourceKeys, targetKeys)
		for _, v := range diffKeys {
			delete(intersectionKeys, v)
		}
	}
	zaplogger.Sugar().Info("Comparing DB Keys finished")
	return intersectionKeys
}

func (compare *CompareDfSingle2Single) ScanKeysToMap(client *redis.Client, dbsize int64, res map[string]bool) {
	zaplogger.Sugar().Info("Scanning keys of ", client.Options().Addr)
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	key_count := uint64(0)

	cursor := uint64(0)
	for {
		result, c, err := client.Scan(cursor, "*", compare.BatchSize).Result()

		if err != nil {
			zaplogger.Sugar().Info(result, c, err)
			return
		}

		for _, key := range result {
			res[key] = true

		}
		key_count += uint64(len(result))
		cursor = c

		if c == 0 {
			break
		}

		select {
		case <-ticker.C:
			zaplogger.Sugar().Info("Scanning keys of ", client.Options().Addr, ", scaned ", key_count, " out of ", dbsize)
		default:
			continue
		}
	}
}

func (compare *CompareDfSingle2Single) FindKeysDiff(sourcekeys map[string]bool, targetkeys map[string]bool) []string {

	var diffKeys []string
	compareresult := NewCompareResult()
	reason := make(map[string]interface{})
	compareresult.IsEqual = false
	compareresult.Source = compare.Source[0].Options().Addr
	compareresult.Target = compare.Target[0].Options().Addr
	compareresult.SourceDB = compare.SourceDB
	compareresult.TargetDB = compare.TargetDB
	reason["description"] = "key does not exists in Target"
	for k, _ := range sourcekeys { // range over source map
		if _, ok := targetkeys[k]; !ok { // check if the key from source map exist in target
			reason["key"] = k
			diffKeys = append(diffKeys, k)
			compareresult.Key = k
			compareresult.KeyDiffReason = append(compareresult.KeyDiffReason, reason)
			zaplogger.Info("", zap.Any("CompareResult", compareresult))
			if compare.RecordResult {
				jsonBytes, _ := json.Marshal(compareresult)
				commons.AppendLineToFile(bytes.NewBuffer(jsonBytes), compare.ResultFile)
			}

		}
	}
	reason["description"] = "key does not exists in Source"
	for k, _ := range targetkeys { // range over target map
		if _, ok := sourcekeys[k]; !ok { // check if the key from target map exist in source
			reason["key"] = k
			compareresult.Key = k
			compareresult.KeyDiffReason = append(compareresult.KeyDiffReason, reason)
			zaplogger.Info("", zap.Any("CompareResult", compareresult))
			if compare.RecordResult {
				jsonBytes, _ := json.Marshal(compareresult)
				commons.AppendLineToFile(bytes.NewBuffer(jsonBytes), compare.ResultFile)
			}
		}
	}
	return diffKeys
}

func (compare *CompareDfSingle2Single) CompareValues(keys map[string]bool) {
	zaplogger.Sugar().Info("Comparing DB keys Values")
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	// Add all keys to channel and creat X workers to read from channel and compare key value between source and target
	keysChannel := make(chan string, compare.BatchSize*1000)
	wg := sync.WaitGroup{}
	wg.Add(compare.CompareThreads)
	for i := 0; i < compare.CompareThreads; i++ {
		go func(id int) {
			compare.CompareKeysWorker(id, keysChannel)
			wg.Done()
		}(i)
	}

	keyCount := 0
	for key, _ := range keys {
		keysChannel <- key
		keyCount++
		select {
		case <-ticker.C:
			zaplogger.Sugar().Info("Comparing values, insert to channel keys: ", keyCount, " out of: ", len(keys))
		default:
			continue
		}

	}
	close(keysChannel)
	wg.Wait()
	zaplogger.Sugar().Info("Comparing DB Values finished")
}

func (compare *CompareDfSingle2Single) CompareKeysWorker(id int, keys <-chan string) {
	zaplogger.Sugar().Info("Comparing values, worker id:", id)
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	srcClient := compare.Source[id]
	targetClient := compare.Target[id]
	batchKeys := []string{}
	keyCount := 0
	// Compare keys in batch, consume batch from channel and call CompareBatch
	// untill there are no keys in channel.
	for key := range keys {
		batchKeys = append(batchKeys, key)
		if len(batchKeys) == int(compare.BatchSize) {
			compare.CompareBatch(batchKeys, srcClient, targetClient)
			keyCount += len(batchKeys)
			batchKeys = []string{}
		}
		select {
		case <-ticker.C:
			zaplogger.Sugar().Info("Comparing values, worker id:", id, " compared keys: ", keyCount)
		default:
			continue
		}

	}
	zaplogger.Sugar().Info("Comparing values, worker id:", id, " finished reading from channel")
	if len(batchKeys) != 0 {
		compare.CompareBatch(batchKeys, srcClient, targetClient)
	}
	zaplogger.Sugar().Info("Comparing values, worker id:", id, " compared keys: ", keyCount)
}

func (compare *CompareDfSingle2Single) CompareBatch(keys []string, srcclient *redis.Client, trgclient *redis.Client) {
	sourceRes := map[string]*redis.StringCmd{}
	sourcePipe := srcclient.Pipeline()
	targetRes := map[string]*redis.StringCmd{}
	targetPipe := trgclient.Pipeline()
	for _, key := range keys {
		sourceRes[key] = sourcePipe.Dump(key)
		targetRes[key] = targetPipe.Dump(key)
	}
	_, err := sourcePipe.Exec()
	if err != nil {
		panic(err)
	}
	_, err = targetPipe.Exec()
	if err != nil {
		panic(err)
	}

	compareresult := NewCompareResult()
	compareresult.Source = srcclient.Options().Addr
	compareresult.Target = trgclient.Options().Addr
	compareresult.SourceDB = compare.SourceDB
	compareresult.TargetDB = compare.TargetDB
	compareresult.IsEqual = false

	for _, key := range keys {
		sourceval, _ := sourceRes[key].Result()
		targetval, _ := targetRes[key].Result()
		if sourceval != targetval {

			reason := make(map[string]interface{})
			reason["description"] = "Different value for key"
			reason["key"] = key
			compareresult.Key = key
			compareresult.KeyDiffReason = append(compareresult.KeyDiffReason, reason)
			zaplogger.Info("", zap.Any("CompareResult", compareresult))
			if compare.RecordResult {
				jsonBytes, _ := json.Marshal(compareresult)
				commons.AppendLineToFile(bytes.NewBuffer(jsonBytes), compare.ResultFile)
			}
		}
	}

}
