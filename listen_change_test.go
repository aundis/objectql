package objectql

import (
	"context"
	"errors"
	"testing"
)

func TestListenChange(t *testing.T) {
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
	objectql.ListenChange("staff", &ListenChangeHandler{
		Listen: []string{"name"},
		Query:  []string{"age"},
		Handle: func(ctx context.Context, change map[string]bool, entity *Var, before *Var) error {
			// fmt.Println("changeMap", change)
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
}
