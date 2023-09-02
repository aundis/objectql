package objectql

import (
	"context"
	"strings"
	"testing"

	"github.com/gogf/gf/v2/util/gconv"
)

func TestObjectPermissionCheck(t *testing.T) {
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
			},
		},
		Comment: "",
	})
	err = objectql.InitObjects()
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	objectql.SetObjectPermissionCheckHandler(func(ctx context.Context, object string, kind PermissionKind) bool {
		return false
	})

	_, err = objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
	})
	if err == nil {
		t.Error("未返回预期错误")
		return
	}
	if !strings.Contains(err.Error(), "permission") {
		t.Error("预期错误不一致", err.Error())
		return
	}
}

func TestObjectFieldPermissionCheck(t *testing.T) {
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
			},
		},
		Comment: "",
	})
	err = objectql.InitObjects()
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 设定对象字段权限检查
	objectql.SetObjectFieldPermissionCheckHandler(func(ctx context.Context, object, field string, kind PermissionKind) bool {
		if kind == FieldUpdate {
			return true
		} else {
			return field == "name"
		}
	})
	// 插入数据
	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
		Fields: Fields{
			"_id",
			"name",
			"age",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err.Error())
		return
	}
	if gconv.String(res["name"]) != "小明" || gconv.Int(res["age"]) != 0 {
		t.Errorf(`预期结果不一致, name="%s", age=%d`, gconv.String(res["name"]), gconv.Int(res["age"]))
		return
	}
	// 删除数据
	err = objectql.Delete(ctx, "student", gconv.String(res["_id"]))
	if err != nil {
		t.Error("删除数据失败", err.Error())
		return
	}
}
