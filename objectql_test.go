package objectql

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/util/guid"
	"go.mongodb.org/mongo-driver/bson"
)

var testMongodbUrl = "mongodb://192.168.0.197:27017/?connect=direct"

type GraphqlQueryReq struct {
	Query     string `json:"query"`
	Variables string `json:"variables"`
}

type getNameReq struct {
	Number int `v:"min:100"`
	Age    int `v:"min:10"`
}

func TestQuery(t *testing.T) {
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
	})
	err = oql.InitObjects(ctx)
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	res, err := oql.Query(ctx, "student", "getName", map[string]any{
		"age":    10,
		"number": 200,
	})
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}
	if res.ToString() != "10,200" {
		t.Errorf("except 10,200 but got %s", res.ToAny())
		return
	}
}

// func TestMutation(t *testing.T) {
// 	ctx := context.Background()
// 	objectql := New()
// 	err := objectql.InitMongodb(ctx, testMongodbUrl, "test")
// 	if err != nil {
// 		t.Error("初始化数据库失败", err)
// 		return
// 	}

// 	objectql.AddObject(&Object{
// 		Name: "学生",
// 		Api:  "student",
// 		Fields: []*Field{
// 			{
// 				Name: "姓名",
// 				Api:  "name",
// 				Type: String,
// 			},
// 			{
// 				Name: "年龄",
// 				Api:  "age",
// 				Type: Int,
// 			},
// 		},
// 		Comment: "",
// 		Querys: []*Query{
// 			{
// 				Name: "获取姓名",
// 				Api:  "getName",
// 				Handle: func(ctx context.Context, req getNameReq) (getNameRes, error) {
// 					return getNameRes{Index: req.Age, Name: fmt.Sprintf("%d,%d", req.Age, req.Number)}, nil
// 				},
// 			},
// 		},
// 	})
// 	err = objectql.InitObjects(ctx)
// 	if err != nil {
// 		t.Error("初始化对象失败", err)
// 		return
// 	}
// }

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

// func TestQuery(t *testing.T) {
// 	oql := New()
// 	oql.AddObject(&objectql.Object{
// 		Name: "任务日志",
// 		Api:  "sysTaskLog",
// 		Fields: []*Field{
// 			{
// 				Name: "任务ID",
// 				Api:  "task",
// 				Type: NewRelate("sysTask"),
// 			},
// 			{
// 				Name: "任务名称",
// 				Api:  "taskName",
// 				Type: String,
// 			},
// 			{
// 				Name: "状态",
// 				Api:  "status",
// 				Type: Bool,
// 			},
// 			{
// 				Name: "描述",
// 				Api:  "detail",
// 				Type: String,
// 			},
// 			{
// 				Name: "消耗时间",
// 				Api:  "consumeTime",
// 				Type: Int,
// 			},
// 		},
// 	})
// }

//  ended session was used

func TestSession(t *testing.T) {
	ctx := context.Background()
	oql := New()
	err := oql.InitMongodb(ctx, testMongodbUrl, "test")
	if err != nil {
		panic(err)
	}
	oql.AddObject(&Object{
		Name: "用户信息",
		Api:  "person22",
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
	err = oql.InitObjects(ctx)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = oql.Insert(ctx, "person22", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = oql.Insert(ctx, "person22", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = oql.Insert(ctx, "person22", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
}

// func TestServer(t *testing.T) {
// 	ctx := context.Background()
// 	objectql := New()
// 	err := objectql.InitMongodb(context.Background(), "mongodb://192.168.0.197:27017/?connect=direct")
// 	if err != nil {
// 		panic(err)
// 	}

// 	objectql.AddObject(&Object{
// 		Name: "人",
// 		Api:  "person",
// 		Fields: []*Field{
// 			{
// 				Name: "名称",
// 				Api:  "name",
// 				Type: String,
// 			},
// 			{
// 				Name: "爱好",
// 				Api:  "aih",
// 				Type: NewArrayType(String),
// 			},
// 		},
// 		Querys: []*Query{
// 			{
// 				Name: "获取姓名",
// 				Api:  "getName",
// 				Handle: func(ctx context.Context, req *getNameReq) (*getNameRes, error) {
// 					// return &getNameRes{Index: req.Age, Name: fmt.Sprintf("%d,%d", req.Age, req.Number)}, nil
// 					return nil, nil
// 				},
// 				// Handle: func(ctx context.Context, req getNameReq) (bool, error) {
// 				// 	return true, nil
// 				// },
// 			},
// 		},
// 	})

// 	// 初始化
// 	err = objectql.InitObjects(ctx)
// 	if err != nil {
// 		panic(err)
// 	}

// 	http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
// 		if r.Method == "POST" {
// 			var params *GraphqlQueryReq
// 			err := json.NewDecoder(r.Body).Decode(&params)
// 			if err != nil {
// 				http.Error(w, err.Error(), http.StatusBadRequest)
// 				return
// 			}

// 			result := objectql.Do(context.Background(), params.Query)
// 			// result := graphql.Do(graphql.Params{
// 			// 	Schema:        objectql.gschema,
// 			// 	RequesString: params.Query,
// 			// 	Context: context.Background(),
// 			// })
// 			json.NewEncoder(w).Encode(result)
// 		} else {
// 			http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
// 		}
// 	})

// 	// 处理GraphQL Playground页面
// 	http.HandleFunc("/", graphiql.ServeGraphiQL)

// 	// 启动服务器
// 	fmt.Println("Listening on1 :8080")
// 	http.ListenAndServe(":8080", nil)
// }

func TestInsert(t *testing.T) {
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

	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
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
	// 查找这个新创建的记录
	one, err := objectql.FindOne(ctx, "student", FindOneOptions{
		Filter: M{
			"_id": M{
				"$toId": id,
			},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if one == nil {
		t.Error("找不到记录")
		return
	}
	// 删除这条记录
	err = objectql.DeleteById(ctx, "student", DeleteByIdOptions{
		ID: id,
	})
	if err != nil {
		t.Error("找不到记录")
		return
	}
}

func TestUpdate(t *testing.T) {
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

	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
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
	// 修改数据
	one, err := objectql.UpdateById(ctx, "student", UpdateByIdOptions{
		ID: id,
		Doc: bson.M{
			"age": 20,
		},
		Fields: []string{
			"age",
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if one.IsNil() {
		t.Error("找不到记录")
		return
	}
	if one.Int("age") != 20 {
		t.Errorf("except age = 20 but got %d", one.Int("age"))
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

func TestDelete(t *testing.T) {
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

	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
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
	// 删除这条数据
	err = objectql.DeleteById(ctx, "student", DeleteByIdOptions{
		ID: id,
	})
	if err != nil {
		t.Error(err)
		return
	}
	// 查找这个新创建的记录
	one, err := objectql.FindOne(ctx, "student", FindOneOptions{
		Filter: map[string]any{
			"_id": id,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if one != nil {
		t.Error("记录删除失败", err)
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
			Api:        "staff",
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
				Call: "staff.insert",
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
				Result: "staff1",
			},
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
					"__index",
				},
				Result: "staff2",
			},
		})
		if err != nil {
			return nil, err
		}

		index1 := res.Int("staff1.__index")
		index2 := res.Int("staff2.__index")
		if index2-index1 != 1 {
			return nil, fmt.Errorf("except index2 - index1 = 1 but got %d", index2-index1)
		}

		return nil, ErrOk
	})
	if err != ErrOk {
		t.Log(err)
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
			Api:        "staff",
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
				Call: "staff.insert",
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
				Result: "staff1",
			},
			{
				Call: "staff.insert",
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
				Result: "staff2",
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{"$formula": "staff1._id"},
				},
				Fields: []string{"__index"},
				Result: "a",
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{"$formula": "staff2._id"},
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
		t.Log(err)
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
			Api:        "staff",
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
				Call: "staff.insert",
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
				Result: "staff1",
			},
			{
				Call: "staff.insert",
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
				Result: "staff2",
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{"$formula": "staff1._id"},
				},
				Fields: []string{"__index"},
				Result: "a",
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{"$formula": "staff2._id"},
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
		t.Log(err)
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
			Api:   "staff",
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
				Call: "staff.insert",
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
				Result: "staff1",
			},
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
					"__index",
				},
				Result: "staff2",
			},
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
					"__index",
				},
				Result: "staff3",
			},
			{
				Call: "staff.move",
				Args: M{
					"id": M{
						"$formula": "staff3._id",
					},
					"index": 1,
					"dir":   1,
				},
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{
						"$formula": "staff1._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "staff1_1",
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{
						"$formula": "staff2._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "staff2_1",
			},
			{
				Call: "staff.findOneById",
				Args: M{
					"id": M{
						"$formula": "staff3._id",
					},
				},
				Fields: []string{
					"_id",
					"__index",
				},
				Result: "staff3_1",
			},
		}, M{
			"a1": M{"$formula": "staff1.__index"},
			"b1": M{"$formula": "staff2.__index"},
			"c1": M{"$formula": "staff3.__index"},
			"a2": M{"$formula": "staff1_1.__index"},
			"b2": M{"$formula": "staff2_1.__index"},
			"c2": M{"$formula": "staff3_1.__index"},
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
		t.Log(err)
		return
	}
}
