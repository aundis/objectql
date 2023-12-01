package objectql

import "testing"

func TestVarPath(t *testing.T) {
	v := NewVar(M{
		"person": M{
			"name": "kangkang",
			"age":  18,
		},
	})
	// t.Log(v.String("person.name"))
	if v.String("person.name") != "kangkang" {
		t.Error("except person.name = 'kangkang'")
		return
	}
	if v.Int("person.age") != 18 {
		t.Error("except person.age = 18")
		return
	}
	if !v.isNull("person.class") {
		t.Error("except person.class = null")
		return
	}
}
