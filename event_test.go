package objectql

import (
	"context"
	"errors"
	"testing"
)

func TestInsertBefore(t *testing.T) {
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
	objectql.ListenInsertBefore("staff", func(ctx context.Context, doc *Var) error {
		return errors.New("禁止创建Before")
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
		t.Error("未返回预期错误")
		return
	}
	if err.Error() != "禁止创建Before" {
		t.Error("预期错误不一致", err.Error())
		return
	}
}

func TestInsertAfter(t *testing.T) {
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
	objectql.ListenInsertAfter("staff", func(ctx context.Context, id string, doc *Var) error {
		return errors.New("禁止创建After")
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
		t.Error("未返回预期错误")
		return
	}
	if err.Error() != "禁止创建After" {
		t.Error("预期错误不一致", err.Error())
		return
	}
}

func TestUpdateBefore(t *testing.T) {
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
	objectql.ListenUpdateBefore("staff", func(ctx context.Context, id string, doc *Var) error {
		return errors.New("禁止更新Before")
	})
	// 插入对象
	res, err := objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
		},
	})
	if err != nil {
		t.Error("插入对象错误", err)
		return
	}
	// 更新对象
	id := res.String("_id")
	_, err = objectql.UpdateById(ctx, "staff", UpdateByIdOptions{
		ID: id,
		Doc: map[string]interface{}{
			"age": 23,
		},
	})
	if err == nil {
		t.Error("未返回预期错误")
		return
	}
	if err.Error() != "禁止更新Before" {
		t.Error("预期错误不一致", err.Error())
		return
	}
	// 删除对象
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: id,
	})
	if err != nil {
		t.Error("删除对象错误", err)
		return
	}
}

func TestUpdateAfter(t *testing.T) {
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
	objectql.ListenUpdateAfter("staff", func(ctx context.Context, id string, doc *Var) error {
		return errors.New("禁止更新After")
	})
	// 插入对象
	res, err := objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
		},
	})
	if err != nil {
		t.Error("插入对象错误", err)
		return
	}
	// 更新对象
	id := res.String("_id")
	_, err = objectql.UpdateById(ctx, "staff", UpdateByIdOptions{
		ID: id,
		Doc: map[string]interface{}{
			"age": 23,
		},
	})
	if err == nil {
		t.Error("未返回预期错误")
		return
	}
	if err.Error() != "禁止更新After" {
		t.Error("预期错误不一致", err.Error())
		return
	}
	// 删除对象
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: id,
	})
	if err != nil {
		t.Error("删除对象错误", err)
		return
	}
}

func TestDeleteBefore(t *testing.T) {
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
	objectql.ListenDeleteBefore("staff", func(ctx context.Context, id string) error {
		return errors.New("禁止删除Before")
	})
	// 插入数据
	res, err := objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
		},
	})
	if err != nil {
		t.Error("插入对象错误", err)
		return
	}
	// 删除对象
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: res.String("_id"),
	})
	if err == nil {
		t.Error("未返回预期错误")
		return
	}
	if err.Error() != "禁止删除Before" {
		t.Error("预期错误不一致", err.Error())
		return
	}
}

func TestDeleteAfter(t *testing.T) {
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
	objectql.ListenDeleteAfter("staff", func(ctx context.Context, id string) error {
		return errors.New("禁止删除After")
	})
	// 插入数据
	res, err := objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
		},
	})
	if err != nil {
		t.Error("插入对象错误", err)
		return
	}
	// 删除对象
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: res.String("_id"),
	})
	if err == nil {
		t.Error("未返回预期错误")
		return
	}
	if err.Error() != "禁止删除After" {
		t.Error("预期错误不一致", err.Error())
		return
	}
}
