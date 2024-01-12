package objectql

import (
	"testing"

	"github.com/gogf/gf/v2/os/gctx"
)

func TestFindAllEx(t *testing.T) {
	ctx := gctx.New()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	oql.AddObject(&Object{
		Name: "食物",
		Api:  "food",
		Fields: []*Field{
			{
				Name: "名字",
				Api:  "name",
				Type: String,
			},
		},
		Comment: "",
	})
	oql.AddObject(&Object{
		Name: "狗",
		Api:  "dog",
		Fields: []*Field{
			{
				Name: "名字",
				Api:  "name",
				Type: String,
			},
			{
				Name: "饲料",
				Api:  "food",
				Type: NewRelate("food"),
			},
		},
		Comment: "",
	})
	oql.AddObject(&Object{
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
			{
				Name: "班级",
				Api:  "class",
				Type: NewRelate("class"),
			},
			{
				Name: "宠物",
				Api:  "dogs",
				Type: NewArrayType(NewRelate("dog")),
			},
		},
		Comment: "",
	})
	oql.AddObject(&Object{
		Name: "班级",
		Api:  "class",
		Fields: []*Field{
			{
				Name: "名称",
				Api:  "name",
				Type: String,
			},
			{
				Name: "等级",
				Api:  "level",
				Type: Int,
			},
			{
				Name: "班主任",
				Api:  "boss",
				Type: NewRelate("boss"),
			},
		},
		Comment: "",
	})
	oql.AddObject(&Object{
		Name: "班主任",
		Api:  "boss",
		Fields: []*Field{
			{
				Name: "名称",
				Api:  "name",
				Type: String,
			},
			{
				Name: "血值",
				Api:  "hp",
				Type: Int,
			},
		},
		Comment: "",
	})
	err = oql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	result, err := oql.DoCommands(ctx, []Command{
		{
			Call: "boss.insert",
			Args: M{
				"doc": M{
					"name": "林冲",
					"hp":   100,
				},
			},
			Result: "boss",
		},
		{
			Call: "class.insert",
			Args: M{
				"doc": M{
					"name":  "六年级二班",
					"level": 10,
					"boss": M{
						"$formula": "boss._id",
					},
				},
			},
			Result: "class",
		},
		{
			Call: "food.insert",
			Args: M{
				"doc": M{
					"name": "大骨头",
				},
			},
			Result: "food1",
		},
		{
			Call: "dog.insert",
			Args: M{
				"doc": M{
					"name": "小白",
					"food": M{
						"$formula": "food1._id",
					},
				},
			},
			Result: "dog1",
		},
		{
			Call: "dog.insert",
			Args: M{
				"doc": M{
					"name": "小黄",
					"food": M{
						"$formula": "food1._id",
					},
				},
			},
			Result: "dog2",
		},
		{
			Call: "student.insert",
			Args: M{
				"doc": M{
					"name": "王晓宇",
					"age":  12,
					"class": M{
						"$formula": "class._id",
					},
					"dogs": M{
						"$formula": "[dog1._id, dog2._id]",
					},
				},
			},
			Result: "student",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	// t.Log(result)
	userId := result.String("student._id")
	t.Log(userId)

	list, err := oql.mongoFindAllEx(ctx, "student", findAllExOptions{
		Fields: []string{
			"_id",
			"name",
			"age",
			"dogs",
			"class__expand._id",
			"class__expand.name",
			"class__expand.level",
			"class__expand.boss__expand._id",
			// "class__expand.boss__expand.hp",
			"class__expand.boss__expand.name",
			// "dogs__expands.name",
			// "dogs__expands.food__expand.name",
		},
		Top: 1,
		Filter: map[string]interface{}{
			"_id":                             ObjectIdFromHex(userId),
			"dogs__expands.name":              "小白",
			"dogs__expands.food__expand.name": "大骨头",
		},
		// Skip:   0,
		// Sort:   []string{},
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(list)
	count, err := oql.mongoCountEx(ctx, "student", countExOptions{
		Filter: map[string]any{
			"dogs__expands.name": "小白",
			// "dogs__expands.food__expand.name": "大骨头",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(count)
	// Write list
	// err = writeJSONToFile("list.json", list)
	// if err != nil {
	// 	t.Error("Error writing result to file:", err)
	// 	return
	// }
}
