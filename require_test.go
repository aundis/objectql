package objectql

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRequire(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl)
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
				Name:       "年龄",
				Api:        "age",
				Type:       Int,
				Require:    "name == '刚子'",
				RequireMsg: "刚子的年龄是必填的",
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
			"name": "刚子",
		},
		Fields: []string{
			"_id",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "字段<年龄>是必填项") {
		t.Error("except 字段<年龄>是必填项 but got", err)
		return
	}
}

func TestHandleRequire(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl)
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
				Require: &FieldReqireCheckHandle{
					Fields: []string{
						"name",
					},
					Handle: func(ctx context.Context, cur *Var) error {
						if cur.String("name") == "刚子" {
							return errors.New("刚子的年龄是必填的")
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
			"name": "刚子",
		},
		Fields: []string{
			"_id",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "刚子的年龄是必填的") {
		t.Error("except 刚子的年龄是必填的 but got", err)
		return
	}
}
