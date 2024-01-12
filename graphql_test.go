package objectql

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestPreprceessMongoMap(t *testing.T) {
	r, err := preprocessMongoMap(primitive.M{
		"dyeDate": primitive.M{
			"$it": primitive.M{
				"$toDate": "2023-11-13T01:14:26.916Z",
			},
			"$gte": primitive.M{
				"$toDate": "2023-11-13T01:14:26.916Z",
			},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(r)
}

func TestTimeFilter(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
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
				Name: "出生日期",
				Api:  "date",
				Type: DateTime,
			},
		},
		Comment: "",
	})
	err = oql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	_, err = oql.Insert(ctx, "student", InsertOptions{
		Doc: M{
			"name": "xun",
			"age":  18,
			"date": time.Now().Format(time.RFC3339),
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	// t.Log(r)
	r, err := oql.FindList(ctx, "student", FindListOptions{
		Filter: M{
			"date": M{
				"$lt": M{
					"$toDate": time.Now().Format(time.RFC3339),
				},
			},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(len(r))
}
