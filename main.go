package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aundis/graphql"
	graphiql "github.com/mnmtanish/go-graphiql"

	"gopkg.in/mgo.v2"
)

var session *mgo.Session

func init() {
	var err error
	session, err = mgo.Dial("mongodb://192.168.0.197:27017")
	if err != nil {
		panic(err)
	}
}

type GraphqlQueryReq struct {
	Query     string `json:"query"`
	Variables string `json:"variables"`
}

type Author struct {
	Name string `json:"name"`
}

func main() {
	Manager().InitObjects([]*Object{
		{
			Name: "作者",
			Api:  "author",
			Fields: []*Field{
				{
					Name:    "标题",
					Api:     "title",
					Type:    String,
					Comment: "",
				},
				{
					Name:    "作者名",
					Api:     "name",
					Type:    String,
					Comment: "",
				},
				{
					Name:    "年龄",
					Api:     "age",
					Type:    Int,
					Comment: "",
				},
				{
					Name: "汽车",
					Api:  "car",
					Type: Relate,
					Data: &RelateData{
						ObjectApi: "car",
					},
				},
				{
					Name: "汽车品牌",
					Api:  "car_brand",
					Type: Formula,
					Data: &FormulaData{
						Formula: "car.brand",
						Type:    String,
					},
				}, {
					Name: "身价",
					Api:  "shen",
					Type: Formula,
					Data: &FormulaData{
						Formula: "age + car.oil + car.speed",
						Type:    Int,
					},
				},
			},
			Comment: "",
		},
		{
			Name: "汽车",
			Api:  "car",
			Fields: []*Field{
				{
					Name:    "品牌",
					Api:     "brand",
					Type:    String,
					Comment: "",
				},
				{
					Name:    "速度",
					Api:     "speed",
					Type:    Int,
					Comment: "",
				},
				{
					Name:    "油耗",
					Api:     "oil",
					Type:    Int,
					Comment: "",
				},
			},
		},
		{
			Name: "教师",
			Api:  "teacher",
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
					Type: String,
				},
				// {
				// 	Name: "学生总数",
				// 	Api:  "student_count",
				// 	Kind: objectql.Aggregation,
				// 	Type: objectql.Int,
				// 	Data: &objectql.AggregationData{

				// 		Relate:    "student.teacher",
				// 		Kind:      objectql.Count,
				// 		Condition: "",
				// 	},
				// },
				{
					Name: "学生平均年龄",
					Api:  "student_age_avg",
					Type: Aggregation,
					Data: &AggregationData{
						Kind:      Avg,
						Type:      Float,
						Object:    "student",
						Relate:    "teacher",
						Field:     "age",
						Condition: "",
					},
				},
			},
		},
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
					Name: "教师",
					Api:  "teacher",
					Type: Relate,
					Data: &RelateData{
						ObjectApi: "teacher",
					},
				},
				{
					Name: "教师姓名",
					Api:  "teacher_name",
					Type: Formula,
					Data: &FormulaData{
						Type:    String,
						Formula: "teacher.name",
					},
				},
			},
		},
	})

	// 定义GraphQL模式
	schema, err := Manager().GetSchema()
	if err != nil {
		panic(err)
	}

	// 处理GraphQL请求
	// http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
	// 	fmt.Println("graphql")

	// 	result := graphql.Do(graphql.Params{
	// 		Schema:        schema,
	// 		RequestString: r.URL.Query().Get("query"),
	// 	})
	// 	json.NewEncoder(w).Encode(result)
	// })
	http.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			var params *GraphqlQueryReq
			err := json.NewDecoder(r.Body).Decode(&params)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			result := graphql.Do(graphql.Params{
				Schema:        schema,
				RequestString: params.Query,
			})
			json.NewEncoder(w).Encode(result)
		} else {
			http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		}
	})

	// 处理GraphQL Playground页面
	http.HandleFunc("/", graphiql.ServeGraphiQL)

	// opts := graphiql.NewOptions("http://localhost:8080/graphql")
	// http.Handle("/playground", graphiql.NewGraphiqlHandler(opts))

	// 启动服务器
	fmt.Println("Listening on :8080")
	http.ListenAndServe(":8080", nil)
}
