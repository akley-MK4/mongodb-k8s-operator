package mongoclient

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func CheckAndInitiateMgoShardReplicaSet(replicaSetId, primaryURI string, secondaryURIs, arbiterURIs []string, log logr.Logger) error {
	mgoURI := fmt.Sprintf("mongodb://%s/?directConnection=true", primaryURI)
	client, errConn := mongo.Connect(options.Client().ApplyURI(mgoURI))
	if errConn != nil {
		return fmt.Errorf("mongo.Connect failed, %v", errConn)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Error(err, "Failed to disconnect a mgo client")
		}
	}()

	adminDB := client.Database("admin")

	rsD := bson.D{
		bson.E{Key: "_id", Value: replicaSetId},
	}

	// Memory Nodes
	var memberNodes bson.A
	// Primary Node
	memberNodes = append(memberNodes, bson.D{
		bson.E{Key: "_id", Value: 0},
		bson.E{Key: "priority", Value: 3},
		bson.E{Key: "host", Value: primaryURI},
	})
	// Secondary Node
	for idx, uri := range secondaryURIs {
		memberNodes = append(memberNodes, bson.D{
			bson.E{Key: "_id", Value: idx + 1},
			bson.E{Key: "priority", Value: 2},
			bson.E{Key: "host", Value: uri},
		})
	}

	// Arbiter Node
	for idx, uri := range arbiterURIs {
		memberNodes = append(memberNodes, bson.D{
			bson.E{Key: "_id", Value: 1 + len(secondaryURIs) + idx},
			bson.E{Key: "priority", Value: 0},
			bson.E{Key: "arbiterOnly", Value: true},
			bson.E{Key: "host", Value: uri},
		})
	}

	rsD = append(rsD, bson.E{Key: "members", Value: memberNodes})

	result := adminDB.RunCommand(context.TODO(), bson.D{bson.E{Key: "replSetInitiate", Value: rsD}})
	var raw bson.Raw
	if err := result.Decode(&raw); err != nil {
		return err
	}

	return nil
}

func CheckAndInitiateMgoConfigServerReplicaSet(replicaSetId, primaryURI string, secondaryURIs []string, log logr.Logger) error {
	mgoURI := fmt.Sprintf("mongodb://%s/?directConnection=true", primaryURI)
	client, errConn := mongo.Connect(options.Client().ApplyURI(mgoURI))
	if errConn != nil {
		return fmt.Errorf("mongo.Connect failed, %v", errConn)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Error(err, "Failed to disconnect a mgo client")
		}
	}()

	adminDB := client.Database("admin")

	rsD := bson.D{
		bson.E{Key: "_id", Value: replicaSetId},
	}

	// Memory Nodes
	var memberNodes bson.A
	// Primary Node
	memberNodes = append(memberNodes, bson.D{
		bson.E{Key: "_id", Value: 0},
		bson.E{Key: "priority", Value: len(secondaryURIs) + 1},
		bson.E{Key: "host", Value: primaryURI},
	},
	)
	// Secondary Nodes
	for idx, secondaryURI := range secondaryURIs {
		memberNodes = append(memberNodes, bson.D{
			bson.E{Key: "_id", Value: idx + 1},
			bson.E{Key: "priority", Value: 1},
			bson.E{Key: "host", Value: secondaryURI},
		})
	}
	rsD = append(rsD, bson.E{Key: "members", Value: memberNodes})

	result := adminDB.RunCommand(context.TODO(), bson.D{bson.E{Key: "replSetInitiate", Value: rsD}})
	var raw bson.Raw
	if err := result.Decode(&raw); err != nil {
		return err
	}

	return nil
}

func CheckAndAddShard(routerURI, replicaSetId string, shardMgoURIs []string, log logr.Logger) error {
	mgoURI := fmt.Sprintf("mongodb://%s/?directConnection=true", routerURI)
	client, errConn := mongo.Connect(options.Client().ApplyURI(mgoURI))
	if errConn != nil {
		return fmt.Errorf("mongo.Connect failed, %v", errConn)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Error(err, "Failed to disconnect a mgo client")
		}
	}()

	adminDB := client.Database("admin")

	d := fmt.Sprintf("%s/%s", replicaSetId, strings.Join(shardMgoURIs, ","))
	result := adminDB.RunCommand(context.TODO(), bson.D{bson.E{Key: "addShard", Value: d}})
	var raw bson.Raw
	if err := result.Decode(&raw); err != nil {
		return err
	}

	return nil
}
