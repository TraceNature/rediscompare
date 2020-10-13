package compare

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"rediscompare/commons"
	"testing"
)

func TestGetinfo(t *testing.T) {
	saddr := "114.67.100.239:6379"
	sopt := &redis.Options{
		Addr: saddr,
		DB:   0, // use default DB
	}
	sopt.Password = "redistest0102"
	sclient := commons.GetGoRedisClient(sopt)

	taddr := "114.67.83.163:16379"
	topt := &redis.Options{
		Addr: taddr,
		DB:   0, // use default DB
	}
	topt.Password = "testredis0102"
	tclient := commons.GetGoRedisClient(topt)

	defer sclient.Close()
	defer tclient.Close()

	fmt.Println(tclient.Ping())
	ce := &CompoareEnvironment{
		Sclinet: sclient,
		Tclient: tclient,
	}

	m := ce.DiffParameters()
	fmt.Println(m)

}
