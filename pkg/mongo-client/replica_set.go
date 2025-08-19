package mongoclient

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func FmtReplicaSetURI(replicaSetId, primaryMgoAddr string, secondaryMgoAddrs, arbiterMgoAddrs []string) string {
	mgoAddrs := []string{primaryMgoAddr}
	mgoAddrs = append(mgoAddrs, secondaryMgoAddrs...)
	mgoAddrs = append(mgoAddrs, arbiterMgoAddrs...)

	uri := fmt.Sprintf("mongodb://%s/?replicaSet=%s", strings.Join(mgoAddrs, ","), replicaSetId)
	return uri
}

func CheckReplicaSet(timeout time.Duration, replicaSetId, primaryMgoAddr string, secondaryMgoAddrs, arbiterMgoAddrs []string) (bool, error) {
	uri := FmtReplicaSetURI(replicaSetId, primaryMgoAddr, secondaryMgoAddrs, arbiterMgoAddrs)
	client, errConn := mongo.Connect(options.Client().ApplyURI(uri))
	if errConn != nil {
		return false, fmt.Errorf("unable to connect the mongodb, %v", errConn)
	}
	defer func() {
		if err := DisconnectWithTimeout(timeout, client); err != nil {
			logger.WarningF("Failed to disconnect a mongodb client, %v", err)
		}
	}()

	result := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "replSetGetStatus", Value: 1}})
	if err := result.Err(); err != nil {
		var targetErr topology.ServerSelectionError
		if errors.As(err, &targetErr) {
			if targetErr.Desc.Kind == description.TopologyKindReplicaSetNoPrimary {
				return false, nil
			}
			return false, targetErr
		}
		return false, err
	}

	var bsonM bson.M
	if err := result.Decode(&bsonM); err != nil {
		return false, err
	}

	if bsonM["ok"].(float64) != 1 {
		return false, errors.New(bsonM["errmsg"].(string))
	}

	if bsonM["set"] != replicaSetId {
		return false, fmt.Errorf("current replica set ID is %s, which is not equal to the detected ID", bsonM["set"])
	}

	members := bsonM["members"].(bson.A)
	findMember := func(name string) (bson.D, bool) {
		for i := 0; i < len(members); i++ {
			d := members[i].(bson.D)
			for _, e := range d {
				if e.Key == "name" && e.Value.(string) == name {
					return d, true
				}
			}
		}
		return nil, false
	}

	checkNodeError := func(mgoAddr string, stateStr string) (retErr error) {
		member, found := findMember(mgoAddr)
		if !found {
			retErr = errors.New("the member dose not exist")
			return
		}

		for _, e := range member {
			if e.Key == "health" {
				if e.Value.(float64) != 1 {
					retErr = errors.New("the member is unhealthy")
					return
				}
			}
			// There is no need to check the stateStr as the node type may have changed
			// if e.Key == "stateStr" {
			// 	if e.Value.(string) != stateStr {
			// 		retErr = fmt.Errorf("the member's state is %v, which is inconsistent with the detected state %s", e.Value, stateStr)
			// 		return
			// 	}
			// }
		}
		return
	}

	if err := checkNodeError(primaryMgoAddr, "PRIMARY"); err != nil {
		return false, nil
	}

	for _, mgoAddr := range secondaryMgoAddrs {
		if err := checkNodeError(mgoAddr, "SECONDARY"); err != nil {
			return false, nil
		}
	}

	for _, mgoAddr := range arbiterMgoAddrs {
		if err := checkNodeError(mgoAddr, "ARBITER"); err != nil {
			return false, nil
		}
	}

	return true, nil
}

func InitiateReplicaSet(timeout time.Duration, replicaSetId, primaryMgoAddr string, secondaryMgoAddrs, arbiterMgoAddrs []string) error {
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", primaryMgoAddr)
	client, errConn := mongo.Connect(options.Client().ApplyURI(uri))
	if errConn != nil {
		return fmt.Errorf("unable to connect the mongodb, %v", errConn)
	}
	defer func() {
		if err := DisconnectWithTimeout(timeout, client); err != nil {
			logger.WarningF("Failed to disconnect a mongodb client, %v", err)
		}
	}()

	rsD := bson.D{
		bson.E{Key: "_id", Value: replicaSetId},
	}

	// Memory Nodes
	var memberNodes bson.A
	// Primary Node
	memberNodes = append(memberNodes, bson.D{
		bson.E{Key: "_id", Value: 0},
		bson.E{Key: "priority", Value: 3},
		bson.E{Key: "host", Value: primaryMgoAddr},
	})
	// Secondary Node
	for idx, addr := range secondaryMgoAddrs {
		memberNodes = append(memberNodes, bson.D{
			bson.E{Key: "_id", Value: idx + 1},
			bson.E{Key: "priority", Value: 2},
			bson.E{Key: "host", Value: addr},
		})
	}

	// Arbiter Node
	for idx, addr := range arbiterMgoAddrs {
		memberNodes = append(memberNodes, bson.D{
			bson.E{Key: "_id", Value: 1 + len(secondaryMgoAddrs) + idx},
			bson.E{Key: "priority", Value: 0},
			bson.E{Key: "arbiterOnly", Value: true},
			bson.E{Key: "host", Value: addr},
		})
	}

	rsD = append(rsD, bson.E{Key: "members", Value: memberNodes})

	result := RunCommandWithTimeout(timeout, client, "admin", bson.D{bson.E{Key: "replSetInitiate", Value: rsD}})
	if err := result.Err(); err != nil {
		targetErr := mongo.CommandError{}
		if errors.As(err, &targetErr) {
			if targetErr.Name == "AlreadyInitialized" {
				return nil
			}
			return targetErr
		}
		return err
	}

	var resp bson.E
	if err := result.Decode(&resp); err != nil {
		return err
	}

	return nil
}
