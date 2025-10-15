package mongoclient

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func RunCommandWithTimeout(timeout time.Duration, client *mongo.Client, dbName string, cmd any, opts ...options.Lister[options.RunCmdOptions]) *mongo.SingleResult {
	ctx, cancelFunc := context.WithTimeoutCause(context.TODO(), timeout, errors.New("ctx timeout"))
	defer cancelFunc()

	return client.Database(dbName).RunCommand(ctx, cmd, opts...)
}

func DisconnectWithTimeout(timeout time.Duration, client *mongo.Client) error {
	ctx, cancelFunc := context.WithTimeoutCause(context.TODO(), timeout, errors.New("ctx timeout"))
	defer cancelFunc()

	return client.Disconnect(ctx)
}

func LockMgoDatabase() error {

	return nil
}

func FindListWithTimeout(timeout time.Duration, client *mongo.Client, dbName, collectionName string, filter, results any, opts ...options.Lister[options.FindOptions]) error {
	ctx, cancelFunc := context.WithTimeoutCause(context.TODO(), timeout, errors.New("ctx timeout"))
	defer cancelFunc()

	cursor, errCursor := client.Database(dbName).Collection(collectionName).Find(ctx, filter, opts...)
	if errCursor != nil {
		return errCursor
	}
	defer func() {
		if err := cursor.Close(context.TODO()); err != nil {
			logger.WarningF("Failed to close the cursor of the collection, dbName: %s, collectionName: %s, err: %v", dbName, collectionName, err)
		}
	}()

	return cursor.All(ctx, results)
}
