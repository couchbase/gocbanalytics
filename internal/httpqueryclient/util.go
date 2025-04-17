package httpqueryclient

func getMapValueString(dict map[string]interface{}, key string, def string) string {
	if dict != nil {
		if val, ok := dict[key]; ok {
			if valStr, ok := val.(string); ok {
				return valStr
			}
		}
	}

	return def
}
