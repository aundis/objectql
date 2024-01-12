package objectql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNext(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
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
	objectql.ListenInsertAfter("staff", func(ctx context.Context, id string, doc *Var) error {
		objectql.Next(ctx, func(ctx context.Context) error {
			// fmt.Println("我被调用了111")
			return errors.New("禁止创建")
		})
		return nil
	})
	// 插入对象
	_, err = objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
		},
	})
	if err == nil {
		t.Error("except error")
		return
	}
	if err.Error() != "禁止创建" {
		t.Error("except erroro '禁止创建', but got : ", err)
		return
	}
}

func TestAsyncNext(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
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
	var count = 0
	objectql.ListenInsertAfter("staff", func(ctx context.Context, id string, doc *Var) error {
		objectql.AsyncNext(ctx, func(ctx context.Context) error {
			time.Sleep(1 * time.Second)
			count++
			return nil
		})
		return nil
	})
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
	if count != 0 {
		t.Error("except count = 0 but got", count)
		return
	}
	time.Sleep(2 * time.Second)
	if count != 1 {
		t.Error("except count = 1 but got", count)
	}
}
