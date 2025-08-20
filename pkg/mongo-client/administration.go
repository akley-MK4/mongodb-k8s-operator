package mongoclient

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func FsyncCluster(timeout time.Duration, routerMgoAddr string) (retErr error) {

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

	retCheckSh := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "fsync", Value: 1}, bson.E{Key: "lock", Value: true}})
	var bsonM bson.M
	if err := retCheckSh.Decode(&bsonM); err != nil {
		retErr = err
		return
	}

	if bsonM["ok"].(float64) != 1 {
		retErr = fmt.Errorf("cmd failed, %v", bsonM["errmsg"])
	}

	return nil
}
