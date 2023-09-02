package objectql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/guid"
	"github.com/mnmtanish/go-graphiql"
	"go.mongodb.org/mongo-driver/bson"
)

var testMongodbUrl = "mongodb://192.168.0.197:27017/?connect=direct"

type GraphqlQueryReq struct {
	Query     string `json:"query"`
	Variables string `json:"variables"`
}

func TestServer(t *testing.T) {
	objectql := New()
	err := objectql.initMongodb(context.Background(), "mongodb://192.168.0.197:27017/?connect=direct")
	if err != nil {
		panic(err)
	}

	objectql.AddObject(&Object{
		Name: "人",
		Api:  "person",
		Fields: []*Field{
			{
				Name: "名称",
				Api:  "name",
				Type: String,
			},
			{
				Name: "出生日期",
				Api:  "date",
				Type: DateTime,
			},
			{
				Name: "年",
				Api:  "year",
				Type: Formula,
				Data: &FormulaData{
					Type:    Int,
					Formula: "year(date)",
				},
			},
			{
				Name: "月",
				Api:  "month",
				Type: Formula,
				Data: &FormulaData{
					Type:    Int,
					Formula: "month(date)",
				},
			},
			{
				Name: "日",
				Api:  "day",
				Type: Formula,
				Data: &FormulaData{
					Type:    Int,
					Formula: "day(date)",
				},
			},
			{
				Name: "和",
				Api:  "sum",
				Type: Formula,
				Data: &FormulaData{
					Type:    Int,
					Formula: "year%1000",
				},
			},
		},
	})

	// 初始化
	err = objectql.InitObjects()
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			var params *GraphqlQueryReq
			err := json.NewDecoder(r.Body).Decode(&params)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			result := objectql.Do(context.Background(), params.Query)
			// result := graphql.Do(graphql.Params{
			// 	Schema:        objectql.gschema,
			// 	RequestString: params.Query,
			// 	Context: context.Background(),
			// })
			json.NewEncoder(w).Encode(result)
		} else {
			http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		}
	})

	// 处理GraphQL Playground页面
	http.HandleFunc("/", graphiql.ServeGraphiQL)

	// 启动服务器
	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}

func TestInsert(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.initMongodb(ctx, testMongodbUrl)
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
	err = objectql.InitObjects()
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
		Fields: []interface{}{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	id := gconv.String(res["_id"])
	if len(id) == 0 {
		t.Error("插入数据失败, id为空")
		return
	}
	// 查找这个新创建的记录
	one, err := objectql.FindOne(ctx, "student", FindOneOptions{
		Condition: bson.M{
			"_id": id,
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
	err = objectql.Delete(ctx, "student", id)
	if err != nil {
		t.Error("找不到记录")
		return
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.initMongodb(ctx, testMongodbUrl)
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
	err = objectql.InitObjects()
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
		Fields: []interface{}{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	id := gconv.String(res["_id"])
	if len(id) == 0 {
		t.Error("插入数据失败, id为空")
		return
	}
	// 修改数据
	one, err := objectql.Update(ctx, "student", id, UpdateOptions{
		Doc: bson.M{
			"age": 20,
		},
		Fields: []interface{}{
			"age",
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
	if gconv.Int(one["age"]) != 20 {
		t.Errorf("except age = 20 but got %d", gconv.Int(one["age"]))
	}
	// 删除这条数据
	err = objectql.Delete(ctx, "student", id)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.initMongodb(ctx, testMongodbUrl)
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
	err = objectql.InitObjects()
	if err != nil {
		t.Error("初始化对象失败", err)
		return
	}

	res, err := objectql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]interface{}{
			"name": "小明",
			"age":  13,
		},
		Fields: []interface{}{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	id := gconv.String(res["_id"])
	if len(id) == 0 {
		t.Error("插入数据失败, id为空")
		return
	}
	// 删除这条数据
	err = objectql.Delete(ctx, "student", id)
	if err != nil {
		t.Error(err)
		return
	}
	// 查找这个新创建的记录
	one, err := objectql.FindOne(ctx, "student", FindOneOptions{
		Condition: bson.M{
			"_id": id,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if one != nil {
		t.Error("记录删除失败")
		return
	}
}

func TestFindOne(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.initMongodb(ctx, testMongodbUrl)
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
	err = objectql.InitObjects()
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
		Fields: []interface{}{
			"_id",
		},
	})
	if err != nil {
		t.Error("插入数据失败", err)
		return
	}
	id := gconv.String(res["_id"])
	if len(id) == 0 {
		t.Error("插入数据失败, id为空")
		return
	}
	// 查找
	one, err := objectql.FindOne(ctx, "student", FindOneOptions{
		Condition: map[string]interface{}{
			"name": name,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}
	if one == nil {
		t.Error("找不到对应数据J")
		return
	}
	// 删除这条数据
	err = objectql.Delete(ctx, "student", id)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestFindList(t *testing.T) {
	ctx := context.Background()
	objectql := New()
	err := objectql.initMongodb(ctx, testMongodbUrl)
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
	err = objectql.InitObjects()
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
			Fields: []interface{}{
				"_id",
			},
		})
		if err != nil {
			t.Error("插入数据失败")
			return
		}
		ids = append(ids, gconv.String(res["_id"]))
	}
	// 查找列表
	list, err := objectql.FindList(ctx, "student", FindListOptions{
		Condition: map[string]interface{}{
			"_id": map[string]interface{}{
				"$in": ids,
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
		err = objectql.Delete(ctx, "student", v)
		if err != nil {
			t.Error("删除数据失败", err)
			return
		}
	}
}
