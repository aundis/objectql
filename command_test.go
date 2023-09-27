package objectql

import (
	"context"
	"testing"
)

func TestDoCommand(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl)
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	oql.AddObject(&Object{
		Name: "账簿",
		Api:  "person",
		Fields: []*Field{
			{
				Name: "名字",
				Api:  "name",
				Type: String,
			}, {
				Name: "年龄",
				Api:  "age",
				Type: Int,
			}, {
				Name: "爱好",
				Api:  "aih",
				Type: NewArrayType(String),
			},
		},
		Comment: "",
	})
	err = oql.InitObjects()
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	res, err := oql.DoCommand(ctx, []Command{
		&InsertCommand{
			Doc: map[string]any{
				"name": "小明",
				"age":  19,
				"aih":  []string{"篮球", "足球"},
			},
			Object: "person",
			Result: "person1",
		},
		&InsertCommand{
			Doc: map[string]any{
				"name": "小红",
				"age":  "$$ person1.age + 10",
				"aih":  []string{"唱歌"},
			},
			Object: "person",
			Result: "person2",
		},
		&InsertCommand{
			Doc: map[string]any{
				"name": "小刚",
				"age":  "$$ person2.age + 10",
				"aih":  "$$ mapToArr([person1, person2], '_id')",
			},
			Object: "person",
			Result: "person3",
		},
		&FindOneByIdCommand{
			Object: "person",
			ID:     "$$ person3._id",
			Result: "last1",
		},
		&FindOneCommand{
			Condition: map[string]any{
				"_id": "$$ last1._id",
			},
			Object: "person",
			Result: "last12",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(res)
}
