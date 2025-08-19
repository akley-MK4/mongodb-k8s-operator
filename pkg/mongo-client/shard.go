package mongoclient

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func CheckShard(timeout time.Duration, routerMgoAddr, replicaSetId string, shardMgoAddrs []string) (retExist bool, retErr error) {
	mgoURI := fmt.Sprintf("mongodb://%s/?directConnection=true", routerMgoAddr)
	client, errConn := mongo.Connect(options.Client().ApplyURI(mgoURI))
	if errConn != nil {
		retErr = fmt.Errorf("mongo.Connect failed, %v", errConn)
		return
	}
	defer func() {
		if err := DisconnectWithTimeout(timeout, client); err != nil {
			logger.WarningF("Failed to disconnect a mgo client, %v", err)
		}
	}()

	retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "listShards", Value: 1}})
	var bsonD bson.M
	if err := retCheckSh.Decode(&bsonD); err != nil {
		retErr = err
		return
	}

	if bsonD["ok"].(float64) != 1 {
		retErr = fmt.Errorf("cmd failed, %v", bsonD["errmsg"])
		return
	}

	shards, ok := bsonD["shards"].(bson.A)
	if !ok {
		retErr = errors.New("unable to convert shards to type bson.A")
		return
	}

	for _, iShard := range shards {
		rsId, existRsId := FindBsonEValueInBsonD("_id", iShard.(bson.D))
		if existRsId && rsId.(string) == replicaSetId {
			retExist = true
			if shState, exit := FindBsonEValueInBsonD("state", iShard.(bson.D)); !exit {
				retErr = errors.New("the state field does not exist")
			} else if shState.(int32) != 1 {
				retErr = fmt.Errorf("the status of this shard is %v, not 1", shState)
			}
			return
		}
	}

	return
}

func AddShard(timeout time.Duration, routerMgoAddr, replicaSetId string, shardMgoAddrs []string) error {
	mgoURI := fmt.Sprintf("mongodb://%s/?directConnection=true", routerMgoAddr)
	client, errConn := mongo.Connect(options.Client().ApplyURI(mgoURI))
	if errConn != nil {
		return fmt.Errorf("mongo.Connect failed, %v", errConn)
	}
	defer func() {
		if err := DisconnectWithTimeout(timeout, client); err != nil {
			logger.WarningF("Failed to disconnect a mgo client, %v", err)
		}
	}()

	d := fmt.Sprintf("%s/%s", replicaSetId, strings.Join(shardMgoAddrs, ","))
	result := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "addShard", Value: d}})
	var raw bson.Raw
	if err := result.Decode(&raw); err != nil {
		return err
	}

	return nil
}

func StopBalancer(timeout time.Duration, routerMgoAddr string) (retErr error) {
	mgoURI := fmt.Sprintf("mongodb://%s/?directConnection=true", routerMgoAddr)
	client, errConn := mongo.Connect(options.Client().ApplyURI(mgoURI))
	if errConn != nil {
		return fmt.Errorf("mongo.Connect failed, %v", errConn)
	}

	defer func() {
		if err := DisconnectWithTimeout(timeout, client); err != nil {
			logger.WarningF("Failed to disconnect a mgo client, %v", err)
		}
	}()

	retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "stopBalancer", Value: 1}})
	var bsonD bson.M
	if err := retCheckSh.Decode(&bsonD); err != nil {
		retErr = err
		return
	}

	if bsonD["ok"].(float64) != 1 {
		retErr = fmt.Errorf("cmd failed, %v", bsonD["errmsg"])
		return
	}

	return nil
}

func RemoveShard(timeout time.Duration, routerMgoAddr, replicaSetId string) error {
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", routerMgoAddr)
	client, errConn := mongo.Connect(options.Client().ApplyURI(uri))
	if errConn != nil {
		return fmt.Errorf("mongo.Connect failed, %v", errConn)
	}
	defer func() {
		if err := DisconnectWithTimeout(timeout, client); err != nil {
			logger.WarningF("Failed to disconnect a mgo client, %v", err)
		}
	}()

	//adminDB := client.Database("admin")

	return nil
}
