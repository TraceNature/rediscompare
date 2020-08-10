package compare

import (
	"github.com/go-redis/redis/v7"
	"rediscompare/commons"
	"testing"
)

func TestCompare_CompareDB(t *testing.T) {
	sopt := &redis.Options{
		Addr: "114.67.67.7:6379",
		DB:   0,
	}
	
	sclient := commons.GetGoRedisClient(sopt)

}

func TestCompareSingle2Single_CompareKeysFromResultFile(t *testing.T) {
	sopt := &redis.Options{
		Addr: "114.67.67.7:6379",
		DB:   0,
	}

	sopt.Password = "redistest0102"

	sclient := commons.GetGoRedisClient(sopt)

	topt := &redis.Options{
		Addr: "114.67.83.163:6379",
		DB:   0,
	}

	topt.Password = "redistest0102"

	tclient := commons.GetGoRedisClient(topt)

	defer sclient.Close()
	defer tclient.Close()

	//check redis 连通性
	//if !commons.CheckRedisClientConnect(sclient) {
	//	cmd.PrintErrln(errors.New("Cannot connect source redis"))
	//	return
	//}
	//if !commons.CheckRedisClientConnect(tclient) {
	//	cmd.PrintErrln(errors.New("Cannot connect target redis"))
	//	return
	//}
	compare := &CompareSingle2Single{
		Source:         sclient,
		Target:         tclient,
		BatchSize:      int64(50),
		TTLDiff:        float64(10000),
		RecordResult:   true,
		CompareThreads: 3,
	}
	compare.CompareKeysFromResultFile([]string{"../compare_20200308173929000.result"})
}
