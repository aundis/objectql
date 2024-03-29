package objectql

import (
	"context"
	"errors"
	"testing"
)

func TestListenChange(t *testing.T) {
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
	count := 0
	objectql.ListenChange("staff", &ListenChangeHandler{
		Listen: []string{"name"},
		Query:  []string{"age"},
		Handle: func(ctx context.Context, change map[string]bool, entity *Var, before *Var) error {
			// fmt.Println("changeMap", change)
			count++
			if !change["name"] {
				return errors.New("except change['name'] = true")
			}
			if before.String("name") != "" {
				return errors.New("except before name is empty")
			}
			if entity.String("name") != "小龙" {
				return errors.New("except after name is '小龙'")
			}
			// fmt.Println("before name value", before.String("name"))
			// fmt.Println("after name value", entity.String("name"))
			return nil
		},
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
	if count == 0 {
		t.Error("except count =  1 but got 0")
		return
	}
}

func TestListenDeleteChange(t *testing.T) {
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
			{
				Name: "特长",
				Api:  "techang",
				Type: NewArrayType(String),
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
	objectql.ListenChange("staff", &ListenChangeHandler{
		Listen: []string{"name"},
		Query:  []string{"age"},
		Handle: func(ctx context.Context, change map[string]bool, entity *Var, before *Var) error {
			return nil
		},
	})
	// 插入对象
	res, err := objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小龙",
			"age":         22,
			"hourly_wage": 100,
			"duration":    8,
			"techang":     []string{"篮球"},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: res.String("_id"),
	})
	if err != nil {
		t.Error(err)
		return
	}
}
