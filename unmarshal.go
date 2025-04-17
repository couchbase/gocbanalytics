package cbcolumnar

import "encoding/json"

// Unmarshaler provides a way to unmarshal data into a Go value.
type Unmarshaler interface {
	// Unmarshal unmarshals the data into the provided value.
	Unmarshal([]byte, interface{}) error
}

// JSONUnmarshaler is an Unmarshaler that performs JSON unmarshalling.
type JSONUnmarshaler struct{}

// NewJSONUnmarshaler creates a new JSONUnmarshaler.
func NewJSONUnmarshaler() *JSONUnmarshaler {
	return &JSONUnmarshaler{}
}

// Unmarshal unmarshals the data into the provided value.
func (ju *JSONUnmarshaler) Unmarshal(data []byte, v interface{}) error {
	if out, ok := v.(*[]byte); ok {
		*out = data

		return nil
	}

	err := json.Unmarshal(data, v)
	if err != nil {
		return unmarshalError{
			Reason: err.Error(),
		}
	}

	return nil
}
