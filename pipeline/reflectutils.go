package pipeline

import (
	"reflect"
)

// TODO: use go1.22 reflect.TypeFor
func isType[T any](t reflect.Type) bool {
	return t == TypeFor[T]()
}

// copied from go1.22 reflect.TypeFor
func TypeFor[T any]() reflect.Type {
	var v T
	if t := reflect.TypeOf(v); t != nil {
		return t // optimize for T being a non-interface kind
	}
	return reflect.TypeOf((*T)(nil)).Elem() // only for an interface kind
}

func asType[T any](v reflect.Value) T {
	return v.Interface().(T)
}

func errOrNil(errVal reflect.Value) error {
	if !isType[error](errVal.Type()) {
		panic("not an error")
	}

	if errVal.IsNil() {
		return nil
	}
	return errVal.Interface().(error)
}
