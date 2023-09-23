package objectql

import (
	"context"
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
	err = objectql.InitObjects()
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
		Fields: []interface{}{
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
	res, err = objectql.UpdateById(ctx, "staff", res.String("_id"), UpdateByIdOptions{
		Doc: map[string]interface{}{
			"hourly_wage": 100,
			"duration":    8,
		},
		Fields: []interface{}{
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
	err = objectql.DeleteById(ctx, "staff", res.String("_id"))
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
				Type: NewFormula(String, "boss.name"),
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
	err = objectql.InitObjects()
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
		Fields: []interface{}{
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
		Fields: []interface{}{
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
		Fields: []interface{}{
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
	res, err = objectql.UpdateById(ctx, "staff", res.String("_id"), UpdateByIdOptions{
		Doc: map[string]interface{}{
			"boss": boss2Id,
		},
		Fields: []interface{}{
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
	res, err = objectql.UpdateById(ctx, "staff", res.String("_id"), UpdateByIdOptions{
		Doc: map[string]interface{}{
			"boss": nil,
		},
		Fields: []interface{}{
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
	err = objectql.DeleteById(ctx, "boss", boss1Id)
	if err != nil {
		t.Error("删除失败", err)
		return
	}
	err = objectql.DeleteById(ctx, "boss", boss2Id)
	if err != nil {
		t.Error("删除失败", err)
		return
	}
	err = objectql.DeleteById(ctx, "staff", res.String("_id"))
	if err != nil {
		t.Error("删除失败", err)
		return
	}
}
