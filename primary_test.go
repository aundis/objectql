package objectql

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestPriamryDuplicate(t *testing.T) {
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
					Name:    "工号",
					Api:     "number",
					Type:    Int,
					Primary: true,
				},
				{
					Name:    "姓名",
					Api:     "name",
					Type:    String,
					Primary: true,
				},
				{
					Name: "年龄",
					Api:  "age",
					Type: Int,
				},
			},
			Comment: "",
		})
		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		_, err = oql.DoCommands(ctx, []Command{
			{
				Call: "staff.insert",
				Args: M{
					"doc": M{
						"number": 13,
						"name":   "老陈",
						"age":    55,
					},
				},
				Fields: []string{
					"_id",
				},
			},
			{
				Call: "staff.insert",
				Args: M{
					"doc": M{
						"number": 13,
						"name":   "老陈",
						"age":    55,
					},
				},
				Fields: []string{
					"_id",
				},
			},
		})
		if err == nil || !strings.Contains(err.Error(), "object staff primary duplicate") {
			return nil, fmt.Errorf("except primary duplicate but got %v", err)
		}

		_, err = oql.DoCommands(ctx, []Command{
			{
				Call: "staff.insert",
				Args: M{
					"doc": M{
						"number": 13,
						"name":   "老陈2",
						"age":    55,
					},
				},
				Fields: []string{
					"_id",
				},
			},
			{
				Call: "staff.insert",
				Args: M{
					"doc": M{
						"number": 15,
						"name":   "老陈2",
						"age":    55,
					},
				},
				Fields: []string{
					"_id",
				},
			},
		})
		if err != nil {
			return nil, err
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Log(err)
		return
	}
}
