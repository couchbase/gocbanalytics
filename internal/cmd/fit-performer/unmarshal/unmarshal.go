package unmarshal

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	cbanalytics "github.com/couchbase/gocbanalytics"
	"github.com/couchbase/gocbanalytics/internal/cmd/fit-performer/protocol/serialization"
)

func NewUnmarshaler(deserializer *serialization.Deserializer) (cbanalytics.Unmarshaler, error) {
	switch d := deserializer.Type.(type) {
	case *serialization.Deserializer_Json:
		return cbanalytics.NewJSONUnmarshaler(), nil
	case *serialization.Deserializer_Custom:
		return &CustomUnmarshaler{}, nil
	default:
		return nil, fmt.Errorf("unsupported deserializer type: %T", d)
	}
}

type CustomUnmarshaler struct {
}

func (c *CustomUnmarshaler) Unmarshal(data []byte, out interface{}) error {
	var m map[string]interface{}

	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}

	m["Serialized"] = false

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, out)
}

func ParseContentAs(as *serialization.ContentAs, row *cbanalytics.QueryResultRow) (*serialization.ContentWas, error) {
	switch as.As.(type) {
	case *serialization.ContentAs_AsList:
		var list []interface{}

		err := row.ContentAs(&list)
		if err != nil {
			return nil, err
		}

		contentWas, err := structpb.NewList(list)
		if err != nil {
			return nil, err
		}

		return &serialization.ContentWas{
			Content: &serialization.ContentWas_ContentWasList{
				ContentWasList: contentWas,
			},
		}, nil
	case *serialization.ContentAs_AsMap:
		var m map[string]interface{}

		err := row.ContentAs(&m)
		if err != nil {
			return nil, err
		}

		contentWas, err := structpb.NewStruct(m)
		if err != nil {
			return nil, err
		}

		return &serialization.ContentWas{
			Content: &serialization.ContentWas_ContentWasMap{
				ContentWasMap: contentWas,
			},
		}, nil
	case *serialization.ContentAs_AsString:
		var m string

		err := row.ContentAs(&m)
		if err != nil {
			return nil, err
		}

		return &serialization.ContentWas{
			Content: &serialization.ContentWas_ContentWasString{
				ContentWasString: m,
			},
		}, nil
	case *serialization.ContentAs_AsByteArray:
		var m json.RawMessage

		err := row.ContentAs(&m)
		if err != nil {
			return nil, err
		}

		return &serialization.ContentWas{
			Content: &serialization.ContentWas_ContentWasBytes{
				ContentWasBytes: m,
			},
		}, nil
	}

	return nil, fmt.Errorf("unsupported content as type")
}
