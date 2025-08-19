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
