package objectql

import (
	"context"
	"errors"
	"testing"
)

func TestInsertAfterEx(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl)
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	objectql.AddObject(&Object{
		Name: "员工",
		Api:  "staff",
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
			{
				Name: "时薪",
				Api:  "hourly_wage",
				Type: Float,
			},
			{
				Name: "时长",
				Api:  "duration",
				Type: Float,
			},
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 添加事件监听
	objectql.ListenInsertAfterEx("staff", &InsertAfterExHandler{
		Fields: []string{
			"age",
			"hourly_wage",
			"duration",
		},
		Handle: func(ctx context.Context, id string, doc *Var, entity *Var) error {
			if entity.Int("age")+entity.Int("hourly_wage")+entity.Int("duration") != 130 {
				return errors.New("age + hourly_wage + duration != 130")
			}
			// fmt.Println(entity.Int("duration"))
			return nil
		},
	})
	// objectql.ListenInsertBefore("staff", func(ctx context.Context, doc *Var) error {
	// 	return errors.New("禁止创建Before")
	// })
	// 插入对象
	_, err = objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	// if err.Error() != "禁止创建Before" {
	// 	t.Error("预期错误不一致", err.Error())
	// 	return
	// }
}

func TestDeleteAfterEx(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl)
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	objectql.AddObject(&Object{
		Name: "员工",
		Api:  "staff",
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
			{
				Name: "时薪",
				Api:  "hourly_wage",
				Type: Float,
			},
			{
				Name: "时长",
				Api:  "duration",
				Type: Float,
			},
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 添加事件监听
	objectql.ListenDeleteAfterEx("staff", &DeleteAfterExHandler{
		Fields: []string{
			"age",
		},
		Handle: func(ctx context.Context, id string, entity *Var) error {
			// fmt.Println(id, entity)
			if entity.Int("age") != 22 {
				return errors.New("age != 22")
			}
			return nil
		},
	})
	// objectql.ListenInsertBefore("staff", func(ctx context.Context, doc *Var) error {
	// 	return errors.New("禁止创建Before")
	// })
	// 插入对象
	one, err := objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: one.String("_id"),
	})
	if err != nil {
		t.Error(err)
		return
	}
	// if err.Error() != "禁止创建Before" {
	// 	t.Error("预期错误不一致", err.Error())
	// 	return
	// }
}
