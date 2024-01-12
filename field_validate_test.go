package objectql

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	objectql.AddObject(&Object{
		Name: "学生",
		Api:  "student",
		Fields: []*Field{
			{
				Name: "姓名",
				Api:  "name",
				Type: String,
			},
			{
				Name:        "年龄",
				Api:         "age",
				Type:        Int,
				Validate:    "age == 18",
				ValidateMsg: "年龄必须18",
			},
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "刚子2",
			"age":  nil,
		},
		Fields: []string{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	_, err = objectql.UpdateById(ctx, "student", UpdateByIdOptions{
		ID: res.String("_id"),
		Doc: map[string]interface{}{
			"age": 17,
		},
		Fields: []string{
			"_id",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "年龄必须18") {
		t.Error("except 年龄必须18 but got", err)
		return
	}
}

func TestHandleValidate(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	objectql.AddObject(&Object{
		Name: "学生",
		Api:  "student",
		Fields: []*Field{
			{
				Name: "姓名",
				Api:  "name",
				Type: String,
			},
			{
				Name: "年龄",
				Api:  "age",
				Type: Int,
				Validate: &FieldValidateHandle{
					Fields: []string{
						"name",
					},
					Handle: func(ctx context.Context, cur *Var) error {
						if cur.Int("age") != 18 {
							return errors.New("年龄必须18")
						}
						return nil
					},
				},
				// RequireMsg: "刚子的年龄是必填的",
			},
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "刚子2",
			"age":  nil,
		},
		Fields: []string{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	_, err = objectql.UpdateById(ctx, "student", UpdateByIdOptions{
		ID: res.String("_id"),
		Doc: map[string]interface{}{
			"age": 17,
		},
		Fields: []string{
			"_id",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "年龄必须18") {
		t.Error("except 年龄必须18 but got", err)
		return
	}
}
