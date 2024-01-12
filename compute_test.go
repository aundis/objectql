package objectql

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// 自身对象的公式字段计算
func TestSelfCompute(t *testing.T) {
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
			{
				Name: "薪资",
				Api:  "salary",
				Type: NewFormula(Float, "hourly_wage * duration"),
			},
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 插入数据
	res, err := objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name":        "小李",
			"age":         25,
			"hourly_wage": 30,
			"duration":    8,
		},
		Fields: []string{
			"_id",
			"salary",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	salary := res.Int("salary")
	if salary != 240 {
		t.Errorf("except hourly wage = 240 bug got %d", salary)
		return
	}
	// 修改数据
	res, err = objectql.UpdateById(ctx, "staff", UpdateByIdOptions{
		ID: res.String("_id"),
		Doc: map[string]interface{}{
			"hourly_wage": 100,
			"duration":    8,
		},
		Fields: []string{
			"_id",
			"salary",
		},
	})
	if err != nil {
		t.Error("修改数据失败", err)
		return
	}
	salary = res.Int("salary")
	if salary != 800 {
		t.Errorf("except hourly wage = 800 bug got %d", salary)
		return
	}
	// 删除数据
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: res.String("_id"),
	})
	if err != nil {
		t.Error("删除失败", err)
		return
	}
}

func TestRelateCompute(t *testing.T) {
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
				Name: "老板",
				Api:  "boss",
				Type: NewRelate("boss"),
			},
			{
				Name: "老板姓名",
				Api:  "boss_name",
				Type: NewFormula(String, "boss__expand.name"),
			},
		},
		Comment: "",
	})
	objectql.AddObject(&Object{
		Name: "老板",
		Api:  "boss",
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
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 添加两个老板
	res, err := objectql.Insert(ctx, "boss", InsertOptions{
		Doc: map[string]interface{}{
			"name": "王健林",
			"age":  60,
		},
		Fields: []string{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	boss1Id := res.String("_id")
	res, err = objectql.Insert(ctx, "boss", InsertOptions{
		Doc: map[string]interface{}{
			"name": "马云",
			"age":  50,
		},
		Fields: []string{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	boss2Id := res.String("_id")
	// 插入数据
	res, err = objectql.Insert(ctx, "staff", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小李",
			"age":  25,
			"boss": boss1Id,
		},
		Fields: []string{
			"_id",
			"boss_name",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	bossName := res.String("boss_name")
	if bossName != "王健林" {
		t.Errorf("except boss_name = 王健林 bug got %s", bossName)
		return
	}
	// 修改数据
	res, err = objectql.UpdateById(ctx, "staff", UpdateByIdOptions{
		ID: res.String("_id"),
		Doc: map[string]interface{}{
			"boss": boss2Id,
		},
		Fields: []string{
			"_id",
			"boss_name",
		},
	})
	if err != nil {
		t.Error("修改数据失败", err)
		return
	}
	bossName = res.String("boss_name")
	if bossName != "马云" {
		t.Errorf("except boss_name = 马云 bug got %s", bossName)
		return
	}
	// 修改数据为空
	res, err = objectql.UpdateById(ctx, "staff", UpdateByIdOptions{
		ID: res.String("_id"),
		Doc: map[string]interface{}{
			"boss": nil,
		},
		Fields: []string{
			"_id",
			"boss_name",
		},
	})
	if err != nil {
		t.Error("修改数据失败", err)
		return
	}
	bossName = res.String("boss_name")
	if bossName != "" {
		t.Errorf("except boss_name is empty bug got %s", bossName)
		return
	}
	// 删除数据
	err = objectql.DeleteById(ctx, "boss", DeleteByIdOptions{
		ID: boss1Id,
	})
	if err != nil {
		t.Error("删除失败", err)
		return
	}
	err = objectql.DeleteById(ctx, "boss", DeleteByIdOptions{
		ID: boss2Id,
	})
	if err != nil {
		t.Error("删除失败", err)
		return
	}
	err = objectql.DeleteById(ctx, "staff", DeleteByIdOptions{
		ID: res.String("_id"),
	})
	if err != nil {
		t.Error("删除失败", err)
		return
	}
}

var ErrOk = errors.New("success")

func TestAggCompute(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl)
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
				{
					Name: "总收入",
					Api:  "sumWages",
					Type: &AggregationType{
						Object: "dayWages",
						Relate: "staff",
						Field:  "wages",
						Type:   Float,
						Kind:   Sum,
						Filter: map[string]any{
							"audit": true,
						},
					},
				},
			},
			Comment: "",
		})

		oql.AddObject(&Object{
			Name: "日工资",
			Api:  "dayWages",
			Fields: []*Field{
				{
					Name: "员工",
					Api:  "staff",
					Type: NewRelate("staff"),
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
				Call: "staff.findOneById",
				Args: M{
					"id": M{
						"$formula": "staff._id",
					},
				},
				Fields: []string{"_id", "name", "age", "sumWages"},
				Result: "staff1",
			},
			{
				Call: "dayWages.updateById",
				Args: M{
					"id": M{
						"$formula": "dw1._id",
					},
					"doc": M{
						"audit": true,
					},
				},
				Fields: []string{"_id"},
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{
						"$formula": "staff._id",
					},
				},
				Fields: []string{"_id", "name", "age", "sumWages"},
				Result: "staff2",
			},
		})
		if err != nil {
			return nil, err
		}

		wages1 := res.Int("staff1.sumWages")
		if wages1 != 30 {
			return nil, fmt.Errorf("except staff1.sumWages = 30 but got %d", wages1)
		}
		wages2 := res.Int("staff2.sumWages")
		if wages2 != 130 {
			return nil, fmt.Errorf("except staff2.sumWages = 130 but got %d", wages2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Log(err)
		return
	}
}
