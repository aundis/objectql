package objectql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/guid"
	"go.mongodb.org/mongo-driver/bson"
)

var testMongodbUrl = "mongodb://192.168.0.197:27017/?connect=direct"

func testTransaction(objects []*Object, fun func(ctx context.Context, oql *Objectql) error) error {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		return gerror.Wrap(err, err.Error())
	}
	for _, object := range objects {
		oql.AddObject(object)
	}
	err = oql.InitObjects(ctx)
	if err != nil {
		return gerror.Wrap(err, err.Error())
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		err = fun(ctx, oql)
		if err != nil {
			return nil, err
		}
		return nil, ErrOk
	})
	if err != ErrOk {
		return gerror.Wrap(err, err.Error())
	}
	return nil
}

func TestQuery(t *testing.T) {
	list := []*Object{
		{
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
			},
			Comment: "",
			Querys: []*Handle{
				{
					Name: "获取姓名",
					Api:  "getName",
					Resolve: func(ctx context.Context, req getNameReq) (string, error) {
						return fmt.Sprintf("%d,%d", req.Age, req.Number), nil
					},
				},
			},
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		res, err := oql.Query(ctx, "student", "getName", map[string]any{
			"age":    10,
			"number": 200,
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		if res.ToString() != "10,200" {
			return gerror.Newf("except 10,200 but got %s", res.ToAny())
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

type getNameReq struct {
	Number int `v:"min:100"`
	Age    int `v:"min:10"`
}

// 引用类型的字段引用不到对象时, 出现空指针引用的问题 #1
func TestIssues1(t *testing.T) {
	oql := New()
	oql.AddObject(&Object{
		Name: "用户信息",
		Api:  "sysUser",
		Fields: []*Field{
			{
				Name: "部门ID",
				Api:  "departmentId",
				Type: NewRelate("xxxxxxxxxxxxx"),
			},
		},
	})
	err := oql.InitObjects(gctx.New())
	if !(err != nil && err.Error() == "can't resolve field 'sysUser.departmentId__expand' type") {
		t.Error("except report error, got: ", err)
	}
}

func TestInsert(t *testing.T) {
	list := []*Object{
		{
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
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		res, err := oql.Insert(ctx, "student", InsertOptions{
			Doc: map[string]interface{}{
				"name": "小明",
				"age":  13,
			},
			Fields: []string{
				"_id",
				"name",
				"age",
			},
		})
		if err != nil {
			return err
		}
		if res.String("name") != "小明" {
			return gerror.Newf("except name = '小明' but got %s", res.String("name"))
		}
		if res.Int("age") != 13 {
			return gerror.Newf("except age = 13 but got %d", res.Int("age"))
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestUpdate(t *testing.T) {
	list := []*Object{
		{
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
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		res, err := oql.Insert(ctx, "student", InsertOptions{
			Doc: map[string]interface{}{
				"name": "小明",
				"age":  13,
			},
			Fields: []string{
				"_id",
			},
		})
		if err != nil {
			return gerror.New(err.Error())
		}
		// 修改数据
		one, err := oql.UpdateById(ctx, "student", UpdateByIdOptions{
			ID: res.String("_id"),
			Doc: bson.M{
				"age": 20,
			},
			Fields: []string{
				"age",
			},
		})
		if err != nil {
			return gerror.New(err.Error())
		}
		if one.IsNil() {
			return gerror.New("没有找到要修改的数据")
		}
		if one.Int("age") != 20 {
			return gerror.Newf("except age = 20 but got %d", one.Int("age"))
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestDelete(t *testing.T) {
	list := []*Object{
		{
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
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		res, err := oql.Insert(ctx, "student", InsertOptions{
			Doc: map[string]interface{}{
				"name": "小明",
				"age":  13,
			},
			Fields: []string{
				"_id",
			},
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		// 删除这条数据
		err = oql.DeleteById(ctx, "student", DeleteByIdOptions{
			ID: res.String("_id"),
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		// 查找这个新创建的记录
		one, err := oql.FindOne(ctx, "student", FindOneOptions{
			Filter: map[string]any{
				"_id": res.String("_id"),
			},
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		if one != nil {
			return gerror.Wrap(err, "记录删除失败")
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestFindOne(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}

	objectql.AddObject(&Object{
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
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 插入数据
	name := guid.S()
	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": name,
			"age":  13,
		},
		Fields: []string{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	id := res.String("_id")
	if len(id) == 0 {
		t.Error("插入数据失败, id为空")
		return
	}
	// 查找
	one, err := objectql.FindOne(ctx, "student", FindOneOptions{
		Filter: map[string]interface{}{
			"name": name,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if one.IsNil() {
		t.Error("找不到对应数据J")
		return
	}
	// 删除这条数据
	err = objectql.DeleteById(ctx, "student", DeleteByIdOptions{
		ID: id,
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestFindList(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	objectql.AddObject(&Object{
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
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 插入几个数据
	var ids []string
	for i := 0; i < 5; i++ {
		name := guid.S()
		res, err := objectql.Insert(ctx, "student", InsertOptions{
			Doc: map[string]interface{}{
				"name": name,
				"age":  13,
			},
			Fields: []string{
				"_id",
			},
		})
		if err != nil {
			t.Error("插入数据失败", err)
			return
		}
		ids = append(ids, res.String("_id"))
	}
	// 查找列表
	list, err := objectql.FindList(ctx, "student", FindListOptions{
		Filter: M{
			"_id": M{
				"$in": IdStrings2ConvMaps(ids),
			},
		},
	})
	if err != nil {
		t.Error("find list err:", err)
		return
	}
	if len(list) != 5 {
		t.Errorf("except find list count = 5 but got %d", len(list))
		return
	}
	// 清空插入的数据
	for _, v := range ids {
		err = objectql.DeleteById(ctx, "student", DeleteByIdOptions{
			ID: v,
		})
		if err != nil {
			t.Error("删除数据失败", err)
			return
		}
	}
}

func TestCount(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	objectql.AddObject(&Object{
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
		},
		Comment: "",
	})
	err = objectql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 插入几个数据
	var ids []string
	for i := 0; i < 5; i++ {
		name := guid.S()
		res, err := objectql.Insert(ctx, "student", InsertOptions{
			Doc: map[string]interface{}{
				"name": name,
				"age":  13,
			},
			Fields: []string{
				"_id",
			},
		})
		if err != nil {
			t.Error("插入数据失败")
			return
		}
		ids = append(ids, res.String("_id"))
	}
	// 查找列表
	count, err := objectql.Count(ctx, "student", CountOptions{
		Filter: map[string]interface{}{
			"name": "小刚",
		},
	})
	if err != nil {
		t.Error("find list err:", err)
		return
	}
	t.Log(count)
	// if len(list) != 5 {
	// 	t.Errorf("except find list count = 5 but got %d", len(list))
	// 	return
	// }
	// 清空插入的数据
	for _, v := range ids {
		err = objectql.DeleteById(ctx, "student", DeleteByIdOptions{
			ID: v,
		})
		if err != nil {
			t.Error("删除数据失败", err)
			return
		}
	}
}

func TestArray(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	oql.AddObject(&Object{
		Name: "记录",
		Api:  "record",
		Fields: []*Field{
			{
				Name: "姓名列表",
				Api:  "names",
				Type: NewArrayType(String),
			},
		},
		Comment: "",
	})
	err = oql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	res, err := oql.Insert(ctx, "record", InsertOptions{
		Doc: map[string]any{
			"names": []string{
				"小明",
				"小李",
				"小金",
			},
		},
	})
	if err != nil {
		t.Error("插入对象失败", err)
		return
	}
	t.Log(res)
}

func TestExtends(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	oql.AddObject(&Object{
		Name: "账簿",
		Api:  "zhangpu",
		Fields: []*Field{
			{
				Name: "记录列表",
				Api:  "records",
				Type: NewArrayType(NewRelate("record")),
			},
		},
		Comment: "",
	})
	oql.AddObject(&Object{
		Name: "记录",
		Api:  "record",
		Fields: []*Field{
			{
				Name: "姓名列表",
				Api:  "names",
				Type: NewArrayType(String),
			},
		},
		Comment: "",
	})
	err = oql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	// 新增5条记录
	var ids []string
	for i := 0; i < 5; i++ {
		res, err := oql.Insert(ctx, "record", InsertOptions{
			Doc: map[string]any{
				"names": []string{"a", "b", "c"},
			},
			Fields: []string{"_id"},
		})
		if err != nil {
			t.Error(err)
			return
		}
		ids = append(ids, res.String("_id"))
	}
	// fmt.Println(ids)
	// 新增一条帐
	res, err := oql.Insert(ctx, "zhangpu", InsertOptions{
		Doc: map[string]any{
			"records": ids,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	// fmt.Println(res.Raw())
	one, err := oql.FindOneById(ctx, "zhangpu", FindOneByIdOptions{
		ID: res.String("_id"),
		Fields: []string{
			"_id",
			"records",
			"records__expands { _id names }",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(one)
}

func TestAggregate(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
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
	err = oql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	_, err = oql.Aggregate(ctx, "person", AggregateOptions{
		Pipeline: []map[string]any{
			{
				"$group": map[string]any{
					"_id": "$name",
					"total": map[string]any{
						"$sum": "$age",
					},
				},
			},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	// t.Log(res)
	// for _, v := range res {
	// 	t.Log(v.ToAny())
	// }
}

func TestWriteGraphqlArgumentValue(t *testing.T) {
	var buffer bytes.Buffer
	str := "hello"
	writeGraphqlArgumentValue(&buffer, &str)
	t.Log(buffer.String())
	t.Log(str)
}

// func TestSort(t *testing.T) {
// 	ctx := context.Background()
// 	oql := New()
// 	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
// 	if err != nil {
// 		t.Error("初始化数据库失败", err)
// 		return
// 	}
// 	oql.AddObject(&Object{
// 		Name: "任务日志",
// 		Api:  "sysTaskLog",
// 		Fields: []*Field{
// 			{
// 				Name: "执行时间",
// 				Api:  "consumeTime",
// 				Type: Int,
// 			},
// 		},
// 		Comment: "",
// 	})
// 	err = oql.InitObjects(ctx)
// 	if err != nil {
// 		t.Error("初始化对象失败", err)
// 		return
// 	}
// 	list, err := oql.FindList(ctx, "sysTaskLog", FindListOptions{
// 		Fields: []string{
// 			"consumeTime",
// 		},
// 		Sort: []string{
// 			"-consumeTime",
// 		},
// 	})
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	t.Log(list)
// }

// func TestFindNull(t *testing.T) {
// 	ctx := context.Background()
// 	objectql := New()
// 	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
// 	if err != nil {
// 		t.Error("初始化数据库失败", err)
// 		return
// 	}
// 	objectql.AddObject(&Object{
// 		Name: "系统日志",
// 		Api:  "sysTaskLog",
// 		Fields: []*Field{
// 			{
// 				Name: "消耗时间",
// 				Api:  "consumeTime",
// 				Type: Int,
// 			},
// 		},
// 		Comment: "",
// 	})
// 	err = objectql.InitObjects(ctx)
// 	if err != nil {
// 		t.Error("初始化对象失败", err)
// 		return
// 	}
// 	// 查找列表
// 	list, err := objectql.FindList(ctx, "sysTaskLog", FindListOptions{
// 		Filter: M{
// 			"consumeTime": M{
// 				"$ne": nil,
// 			},
// 		},
// 	})
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	t.Log(list)
// }

func TestInsertMaxIndex(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:       "员工",
			Api:        "lperson",
			Index:      true,
			IndexGroup: []string{"class"},
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
					Name: "班组",
					Api:  "class",
					Type: Int,
				},
			},
			Comment: "",
		})

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
		})
		if err != nil {
			return nil, err
		}

		index1 := res.Int("lperson1.__index")
		index2 := res.Int("lperson2.__index")
		if index2-index1 != 1 {
			return nil, fmt.Errorf("except index2 - index1 = 1 but got %d", index2-index1)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestInsertDown(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:       "员工",
			Api:        "lperson",
			Index:      true,
			IndexGroup: []string{"class"},
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
					Name: "班组",
					Api:  "class",
					Type: Int,
				},
			},
			Comment: "",
		})

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": 1,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": 1,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson1._id"},
				},
				Fields: []string{"__index"},
				Result: "a",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson2._id"},
				},
				Fields: []string{"__index"},
				Result: "b",
			},
		})
		if err != nil {
			return nil, err
		}

		index1 := res.Int("a.__index")
		index2 := res.Int("b.__index")
		if !(index1 > index2) {
			return nil, fmt.Errorf("except index1 > index2 but got index1=%d index2=%d", index1, index2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestInsertUp(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:       "员工",
			Api:        "lperson",
			Index:      true,
			IndexGroup: []string{"class"},
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
					Name: "班组",
					Api:  "class",
					Type: Int,
				},
			},
			Comment: "",
		})

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": 1,
					"dir":   -1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": 1,
					"dir":   -1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson1._id"},
				},
				Fields: []string{"__index"},
				Result: "a",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson2._id"},
				},
				Fields: []string{"__index"},
				Result: "b",
			},
		})
		if err != nil {
			return nil, err
		}

		index1 := res.Int("a.__index")
		index2 := res.Int("b.__index")
		if !(index2 > index1) {
			return nil, fmt.Errorf("except index2 > index1 but got index1=%d index2=%d", index1, index2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestMoveDown(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:  "员工",
			Api:   "lperson",
			Index: true,
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

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3",
			},
			{
				Call: "lperson.move",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
					"index": 1,
					"dir":   1,
				},
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson1._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson2._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3_1",
			},
		}, M{
			"a1": M{"$formula": "lperson1.__index"},
			"b1": M{"$formula": "lperson2.__index"},
			"c1": M{"$formula": "lperson3.__index"},
			"a2": M{"$formula": "lperson1_1.__index"},
			"b2": M{"$formula": "lperson2_1.__index"},
			"c2": M{"$formula": "lperson3_1.__index"},
		})
		if err != nil {
			return nil, err
		}

		a1 := res.Int("a1")
		b1 := res.Int("b1")
		c1 := res.Int("c1")
		if !(a1 < b1 && b1 < c1) {
			return nil, fmt.Errorf("excecpt a1 < b1 < c1, but got a1=%d  b1=%d c1=%d", a1, b1, c1)
		}

		a2 := res.Int("a2")
		b2 := res.Int("b2")
		c2 := res.Int("c2")
		if !(c2 < a2 && a2 < b2) {
			return nil, fmt.Errorf("excecpt c2 < a2 < b2, but got a2=%d  b2=%d c2=%d", a2, b2, c2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestMoveDownUp(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:  "员工",
			Api:   "lperson",
			Index: true,
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

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3",
			},
			{
				Call: "lperson.move",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
					"index": 1,
					"dir":   -1,
				},
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson1._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson2._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3_1",
			},
		}, M{
			"a1": M{"$formula": "lperson1.__index"},
			"b1": M{"$formula": "lperson2.__index"},
			"c1": M{"$formula": "lperson3.__index"},
			"a2": M{"$formula": "lperson1_1.__index"},
			"b2": M{"$formula": "lperson2_1.__index"},
			"c2": M{"$formula": "lperson3_1.__index"},
		})
		if err != nil {
			return nil, err
		}

		a1 := res.Int("a1")
		b1 := res.Int("b1")
		c1 := res.Int("c1")
		if !(a1 < b1 && b1 < c1) {
			return nil, fmt.Errorf("excecpt a1 < b1 < c1, but got a1=%d  b1=%d c1=%d", a1, b1, c1)
		}

		a2 := res.Int("a2")
		b2 := res.Int("b2")
		c2 := res.Int("c2")
		if !(a2 < c2 && c2 < b2) {
			return nil, fmt.Errorf("excecpt a2 < c2 < b2, but got a2=%d  b2=%d c2=%d", a2, b2, c2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestInsertDownAbs(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:       "员工",
			Api:        "lperson",
			Index:      true,
			IndexGroup: []string{"class"},
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
					Name: "班组",
					Api:  "class",
					Type: Int,
				},
			},
			Comment: "",
		})

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -5,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index":    1,
					"dir":      1,
					"absolute": true,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson1._id"},
				},
				Fields: []string{"__index"},
				Result: "a",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson2._id"},
				},
				Fields: []string{"__index"},
				Result: "b",
			},
		})
		if err != nil {
			return nil, err
		}

		index1 := res.Int("a.__index")
		index2 := res.Int("b.__index")
		if !(index1 > index2 && index2 < 0) {
			return nil, fmt.Errorf("except index1 > index2 && index2 < 0	 but got index1=%d index2=%d", index1, index2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestInsertUpAbs(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:       "员工",
			Api:        "lperson",
			Index:      true,
			IndexGroup: []string{"class"},
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
					Name: "班组",
					Api:  "class",
					Type: Int,
				},
			},
			Comment: "",
		})

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -5,
					"dir":   -1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index":    1,
					"dir":      -1,
					"absolute": true,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson1._id"},
				},
				Fields: []string{"__index"},
				Result: "a",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{"$formula": "lperson2._id"},
				},
				Fields: []string{"__index"},
				Result: "b",
			},
		})
		if err != nil {
			return nil, err
		}

		index1 := res.Int("a.__index")
		index2 := res.Int("b.__index")
		if !(index2 > index1 && index1 < 0 && index2 < 0) {
			return nil, fmt.Errorf("except index2 > index1 && index1 < 0 && index2 < 0 but got index1=%d index2=%d", index1, index2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestMoveDownAbs(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:  "员工",
			Api:   "lperson",
			Index: true,
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

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -10,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -9,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -8,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3",
			},
			{
				Call: "lperson.move",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
					"index":    1,
					"dir":      1,
					"absolute": true,
				},
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson1._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson2._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3_1",
			},
		}, M{
			"a1": M{"$formula": "lperson1.__index"},
			"b1": M{"$formula": "lperson2.__index"},
			"c1": M{"$formula": "lperson3.__index"},
			"a2": M{"$formula": "lperson1_1.__index"},
			"b2": M{"$formula": "lperson2_1.__index"},
			"c2": M{"$formula": "lperson3_1.__index"},
		})
		if err != nil {
			return nil, err
		}

		a1 := res.Int("a1")
		b1 := res.Int("b1")
		c1 := res.Int("c1")
		if !(a1 < b1 && b1 < c1 && c1 < 0) {
			return nil, fmt.Errorf("excecpt a1 < b1 < c1 && c1 < 0, but got a1=%d  b1=%d c1=%d", a1, b1, c1)
		}

		a2 := res.Int("a2")
		b2 := res.Int("b2")
		c2 := res.Int("c2")
		if !(c2 < a2 && a2 < b2 && b2 < 0) {
			return nil, fmt.Errorf("excecpt c2 < a2 < b2 && b2 < 0, but got a2=%d  b2=%d c2=%d", a2, b2, c2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestMoveDownUpAbs(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		t.Error("初始化数据库失败", err)
		return
	}
	_, err = oql.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		oql.AddObject(&Object{
			Name:  "员工",
			Api:   "lperson",
			Index: true,
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

		err = oql.InitObjects(ctx)
		if err != nil {
			return nil, fmt.Errorf("初始化对象失败 %s", err.Error())
		}

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -10,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -9,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2",
			},
			{
				Call: "lperson.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
					"index": -8,
					"dir":   1,
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3",
			},
			{
				Call: "lperson.move",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
					"index":    1,
					"dir":      -1,
					"absolute": true,
				},
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson1._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson1_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson2._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson2_1",
			},
			{
				Call: "lperson.findOneById",
				Args: M{
					"id": M{
						"$formula": "lperson3._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "lperson3_1",
			},
		}, M{
			"a1": M{"$formula": "lperson1.__index"},
			"b1": M{"$formula": "lperson2.__index"},
			"c1": M{"$formula": "lperson3.__index"},
			"a2": M{"$formula": "lperson1_1.__index"},
			"b2": M{"$formula": "lperson2_1.__index"},
			"c2": M{"$formula": "lperson3_1.__index"},
		})
		if err != nil {
			return nil, err
		}

		a1 := res.Int("a1")
		b1 := res.Int("b1")
		c1 := res.Int("c1")
		if !(a1 < b1 && b1 < c1 && c1 < 0) {
			return nil, fmt.Errorf("excecpt a1 < b1 < c1 && c1 < 0, but got a1=%d  b1=%d c1=%d", a1, b1, c1)
		}

		a2 := res.Int("a2")
		b2 := res.Int("b2")
		c2 := res.Int("c2")
		if !(a2 < c2 && c2 < b2 && b2 < 0) {
			return nil, fmt.Errorf("excecpt a2 < c2 < b2 && b2 < 0, but got a2=%d  b2=%d c2=%d", a2, b2, c2)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Error(err)
		return
	}
}

func TestGetObjectMetaInfo(t *testing.T) {
	list := []*Object{
		{
			Name:       "学生",
			Api:        "student",
			Index:      true,
			IndexGroup: []string{"name"},
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
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		info := oql.GetObjectMetaInfo("student")
		if info == nil {
			return errors.New("except info")
		}
		if !info.Index {
			return errors.New("except index = true")
		}
		if len(info.IndexGroup) != 1 {
			return errors.New("except index group array length = 1")
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestCustomerFormulaFunction(t *testing.T) {
	list := []*Object{
		{
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
					Name: "计算值",
					Api:  "value",
					Type: NewFormula(Int, "add10(age)"),
				},
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		oql.AddFormulaFunction("add10", func(v int) (int, error) {
			return v + 10, nil
		})

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "student.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
				},
				Result: "student1",
			},
			{
				Call: "student.findOneById",
				Args: M{
					"id": M{"$formula": "student1._id"},
				},
				Fields: []string{
					"_id",
					"value",
				},
				Result: "student1_1",
			},
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		if res.Int("student1_1.value") != 65 {
			return gerror.Newf("except 65 but got %d", res.Int("student1_1.value"))
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestImmediateFormula(t *testing.T) {
	list := []*Object{
		{
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
					Name: "计算值",
					Api:  "value",
					Type: NewFormula(Int, "10 + 20"),
				},
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {

		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "student.insert",
				Args: M{
					"doc": M{
						"name": "老陈",
						"age":  55,
					},
				},
				Fields: []string{
					"_id",
				},
				Result: "student1",
			},
			{
				Call: "student.findOneById",
				Args: M{
					"id": M{"$formula": "student1._id"},
				},
				Fields: []string{
					"_id",
					"value",
				},
				Result: "student1_1",
			},
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		if res.Int("student1_1.value") != 30 {
			return gerror.Newf("except 30 but got %d", res.Int("student1_1.value"))
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestInitFormulaRelations(t *testing.T) {
	list := []*Object{
		{
			Name: "a",
			Api:  "a",
			Fields: []*Field{
				{
					Name: "编号",
					Api:  "number",
					Type: NewFormula(String, "b__expnad.number + b__expand.number"),
				},
				{
					Name: "b",
					Api:  "b",
					Type: NewRelate("b"),
				},
			},
			Comment: "",
		},
		{
			Name: "b",
			Api:  "b",
			Fields: []*Field{
				{
					Name: "编号",
					Api:  "number",
					Type: String,
				},
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		b, err := oql.MustGetObject("b")
		if err != nil {
			return err
		}
		field := FindFieldFromObject(b, "number")
		if len(field.relations) != 1 {
			return gerror.Newf("except 1 but got %d", len(field.relations))
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestFieldResolve(t *testing.T) {
	list := []*Object{
		{
			Name: "a",
			Api:  "a",
			Fields: []*Field{
				{
					Name: "age",
					Api:  "age",
					Type: Int,
				},
				{
					Name:   "value",
					Api:    "value",
					Type:   Int,
					Fields: []string{"b"},
					Resolve: func(m map[string]any) (interface{}, error) {
						return gconv.Int(m["age"]) + 100, nil
					},
				},
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "a.insert",
				Args: M{
					"doc": M{
						"age": 30,
					},
				},
				Result: "r1",
			},
			{
				Call: "a.findOneById",
				Args: M{
					"id": M{"$formula": "r1._id"},
				},
				Fields: []string{
					"_id",
					"age",
					"value",
				},
				Result: "r2",
			},
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		if res.Int("r2.value") != 130 {
			return gerror.Newf("except 130 but got %d", res.Int("r2.value"))
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestSave(t *testing.T) {
	list := []*Object{
		{
			Name: "学生",
			Api:  "student",
			Fields: []*Field{
				{
					Name:    "学号",
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
					Name:    "班级",
					Api:     "class",
					Type:    String,
					Primary: true,
				},
				{
					Name: "成绩",
					Api:  "source",
					Type: Int,
				},
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		res, err := oql.DoCommands(ctx, []Command{
			{
				Call: "student.insert",
				Args: M{
					"doc": M{
						"number": 1,
						"name":   "小黄",
						"class":  "三年二班",
						"source": 99,
					},
				},
				Result: "r1",
			},
			{
				Call: "student.save",
				Args: M{
					"doc": M{
						"number": 1,
						"name":   "小黄",
						"class":  "三年二班",
						"source": 100,
					},
				},
				Fields: []string{
					"_id",
					"source",
				},
				Result: "r2",
			},
		})
		if err != nil {
			return gerror.Wrap(err, err.Error())
		}
		if res.String("r1._id") != res.String("r2._id") {
			return gerror.New("except _id equal")
		}
		if res.Int("r2.source") != 100 {
			return gerror.New("except r2 source = 100")
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}
