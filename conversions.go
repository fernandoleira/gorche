package gorche

import (
	"fmt"
	"reflect"
	"strconv"
)

func ModelToMap(sch interface{}) (map[string]string, error) {
	t := reflect.TypeOf(sch).Elem()
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("model with type %v is unsupported. Only struct models are supported", t.Kind().String())
	}

	m := make(map[string]string)
	v := reflect.ValueOf(sch).Elem()
	for i := 0; i < v.NumField(); i++ {
		key := t.Field(i)
		val := v.Field(i)

		switch val.Kind() {
		case reflect.String:
			m[key.Name] = val.String()
		case reflect.Bool:
			m[key.Name] = strconv.FormatBool(v.Bool())
		case reflect.Int:
			m[key.Name] = strconv.FormatInt(val.Int(), 10)
		case reflect.Uint:
			m[key.Name] = strconv.FormatUint(val.Uint(), 10)
		default:
			return nil, fmt.Errorf("unexpected type %v received", v.Kind().String())
		}
	}

	return m, nil
}

func MapToModel(data map[string]string, dest interface{}) error {
	return fmt.Errorf("unimplemented")
}
