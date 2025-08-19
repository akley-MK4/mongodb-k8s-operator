package mongoclient

import "go.mongodb.org/mongo-driver/v2/bson"

func FindBsonEValueInBsonD(key string, bsonD bson.D) (interface{}, bool) {
	for _, bsonE := range bsonD {
		if bsonE.Key == key {
			return bsonE.Value, true
		}
	}

	return nil, false
}
