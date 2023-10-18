package objectql

import (
	"context"
	"strings"
	"testing"
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
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	objectql.SetObjectPermissionCheckHandler(func(ctx context.Context, object string, kind PermissionKind) (bool, error) {
		return false, nil
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
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 设定对象字段权限检查
	objectql.SetObjectFieldPermissionCheckHandler(func(ctx context.Context, object, field string, kind PermissionKind) (bool, error) {
		if kind == FieldUpdate {
			return true, nil
		} else {
			return field == "name" || field == "_id", nil
		}
	})
	// 插入数据
	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
		Fields: []string{
			"_id",
			"name",
			"age",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err.Error())
		return
	}
	if res.String("name") != "小明" || res.Int("age") != 0 {
		t.Errorf(`预期结果不一致, name="%s", age=%d`, res.String("name"), res.Int("age"))
		return
	}
	// 删除数据
	// err = objectql.DeleteById(ctx, "student", res.String("_id"))
	// if err != nil {
	// 	t.Error("删除数据失败", err.Error())
	// 	return
	// }
}

// func TestTime(t *testing.T) {
// 	ctx := context.Background()
// 	oql := New()
// 	err := oql.InitMongodb(ctx, testMongodbUrl)
// 	if err != nil {
// 		t.Error("初始化数据库失败", err)
// 		return
// 	}

// 	oql.AddObject(&Object{
// 		Name: "任务日志",
// 		Api:  "sysTaskLog",
// 		Fields: []*Field{
// 			{
// 				Name: "任务名称",
// 				Api:  "taskName",
// 				Type: String,
// 			},
// 			{
// 				Name: "状态",
// 				Api:  "status",
// 				Type: Bool,
// 			},
// 			{
// 				Name: "描述",
// 				Api:  "detail",
// 				Type: String,
// 			},
// 			{
// 				Name: "消耗时间",
// 				Api:  "consumeTime",
// 				Type: Int,
// 			},
// 		},
// 	})

// 	err = oql.InitObjects(ctx)
// 	if err != nil {
// 		t.Error("初始化对象失败", err)
// 		return
// 	}

// 	for i := 0; i < 100; i++ {
// 		start := time.Now().UnixMilli()
// 		list, err := oql.FindList(ctx, "sysTaskLog", FindListOptions{})
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}
// 		count, err := oql.Count(ctx, "sysTaskLog", CountOptions{})
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}
// 		consumeTime := time.Now().UnixMilli() - start
// 		t.Logf("消耗时间：%dms 列表个数: %d, %d", consumeTime, len(list), count)
// 	}
// }
