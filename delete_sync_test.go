package objectql

import (
	"context"
	"fmt"
	"testing"
)

func TestDeleteSync(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
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
			},
			Comment: "",
		})

		oql.AddObject(&Object{
			Name: "日工资",
			Api:  "dayWages",
			Fields: []*Field{
				{
					Name:       "员工",
					Api:        "staff",
					Type:       NewRelate("staff"),
					DeleteSync: true,
				},
				{
					Name: "工资",
					Api:  "wages",
					Type: Int,
				},
				{
					Name: "审核",
					Api:  "audit",
					Type: Bool,
				},
			},
		})
		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "staff.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
				},
				Result: "staff",
			}, {
				Call: "dayWages.insert",
				Args: M{
					"doc": M{
						"staff": M{
							"$formula": "staff._id",
						},
						"wages": 100,
						"audit": false,
					},
				},
				Fields: []string{"_id"},
				Result: "dw1",
			},
			{
				Call: "dayWages.insert",
				Args: M{
					"doc": M{
						"staff": M{
							"$formula": "staff._id",
						},
						"wages": 10,
						"audit": true,
					},
				},
				Fields: []string{"_id"},
				Result: "dw2",
			},
			{
				Call: "dayWages.insert",
				Args: M{
					"doc": M{
						"staff": M{
							"$formula": "staff._id",
						},
						"wages": 20,
						"audit": true,
					},
				},
				Fields: []string{"_id"},
				Result: "dw3",
			},
			{
				Call: "dayWages.count",
				Args: M{
					"filter": M{
						"staff": M{
							"$toId": M{
								"$formula": "staff._id",
							},
						},
					},
				},
				Result: "count1",
			},
			{
				Call: "staff.deleteById",
				Args: M{
					"id": M{
						"$formula": "staff._id",
					},
				},
			},
			{
				Call: "dayWages.count",
				Args: M{
					"filter": M{
						"staff": M{
							"$toId": M{
								"$formula": "staff._id",
							},
						},
					},
				},
				Result: "count2",
			},
		})
		if err != nil {
			return nil, err
		}

		count1 := res.Int("count1")
		if count1 != 3 {
			return nil, fmt.Errorf("except count1 = 3 but got %d", count1)
		}
		count2 := res.Int("count2")
		if count2 != 0 {
			return nil, fmt.Errorf("except count2 = 0 but got %d", count2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Log(err)
		return
	}
}
