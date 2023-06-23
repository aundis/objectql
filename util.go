package main

import "github.com/aundis/graphql"

func formatNullValue(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		if _, ok := v.(graphql.NullValue); ok {
			m[k] = nil
		}
	}
	return m
}
