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

func GetShards(timeout time.Duration, routerMgoAddr string) (retShards bson.A, retErr error) {
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", routerMgoAddr)
	client, errConn := mongo.Connect(options.Client().ApplyURI(uri))
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

	retShards = shards
	return
}

func CheckShardAdded(timeout time.Duration, routerMgoAddr, replicaSetId string, shardMgoAddrs []string) (retExist bool, retErr error) {
	// mgoURI := fmt.Sprintf("mongodb://%s/?directConnection=true", routerMgoAddr)
	// client, errConn := mongo.Connect(options.Client().ApplyURI(mgoURI))
	// if errConn != nil {
	// 	retErr = fmt.Errorf("mongo.Connect failed, %v", errConn)
	// 	return
	// }
	// defer func() {
	// 	if err := DisconnectWithTimeout(timeout, client); err != nil {
	// 		logger.WarningF("Failed to disconnect a mgo client, %v", err)
	// 	}
	// }()

	// retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "listShards", Value: 1}})
	// var bsonD bson.M
	// if err := retCheckSh.Decode(&bsonD); err != nil {
	// 	retErr = err
	// 	return
	// }

	// if bsonD["ok"].(float64) != 1 {
	// 	retErr = fmt.Errorf("cmd failed, %v", bsonD["errmsg"])
	// 	return
	// }

	// shards, ok := bsonD["shards"].(bson.A)
	// if !ok {
	// 	retErr = errors.New("unable to convert shards to type bson.A")
	// 	return
	// }

	shards, errShards := GetShards(timeout, routerMgoAddr)
	if errShards != nil {
		retErr = errShards
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

func CheckBalancerStatusStopped(timeout time.Duration, routerMgoAddr string) (retStopped bool, retErr error) {
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

	retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "balancerStatus", Value: 1}})
	var bsonM bson.M
	if err := retCheckSh.Decode(&bsonM); err != nil {
		retErr = err
		return
	}

	if bsonM["ok"].(float64) != 1 {
		retErr = fmt.Errorf("cmd failed, %v", bsonM["errmsg"])
	}

	retStopped = bsonM["mode"] == "off"
	return
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

	retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "balancerStop", Value: 1}})
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

func RemoveShard(timeout time.Duration, routerMgoAddr, replicaSetId string) (retErr error) {
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

	retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "removeShard", Value: replicaSetId}})
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

func SafeRemoveShard(timeout time.Duration, routerMgoAddr, replicaSetId string) error {
	if shards, err := GetShards(timeout, routerMgoAddr); err != nil {
		return fmt.Errorf("unable to get the shards, %v", err)
	} else if len(shards) <= 1 {
		return errors.New("operation not allowed because it would remove the last shard")
	}

	// Stop the balancer
	if stopped, err := CheckBalancerStatusStopped(timeout, routerMgoAddr); err != nil {
		return fmt.Errorf("checking balancer error, %v", err)
	} else if !stopped {
		if err := StopBalancer(timeout, routerMgoAddr); err != nil {
			return fmt.Errorf("unable to stop the balancer, %v", err)
		}
	}

	// Check if the shard is the primary shard
	if isPrimary, err := CheckShardIsPrimary(timeout, routerMgoAddr, replicaSetId); err != nil {
		return fmt.Errorf("checking primary shard error, %v", err)
	} else if isPrimary {
		//if e := MovePrimaryShard()
	}

	if err := RemoveShard(timeout, routerMgoAddr, replicaSetId); err != nil {
		return err
	}

	return nil
}

type ResultConfigDatabase struct {
	Primary string `bson:"primary"`
}

func CheckShardIsPrimary(timeout time.Duration, routerMgoAddr, replicaSetId string) (isPrimary bool, retErr error) {
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

	var shards []ResultConfigDatabase
	if err := FindListWithTimeout(timeout, client, "config", "databases", bson.M{}, &shards); err != nil {
		retErr = fmt.Errorf("unable to find the shard doc in the config database, %v", err)
		return
	}
	for _, shard := range shards {
		if shard.Primary == replicaSetId {
			isPrimary = true
			return
		}
	}

	return
}

func MovePrimaryShard(timeout time.Duration, routerMgoAddr, srcReplicaSetId, dstReplicaSetId string) (retErr error) {
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

	retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "movePrimary", Value: srcReplicaSetId}, bson.E{Key: "to", Value: dstReplicaSetId}})
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
