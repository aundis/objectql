package objectql

// func TestDoCommand(t *testing.T) {
// 	ctx := context.Background()
// 	oql := New()
// 	err := oql.InitMongodb(ctx, testMongodbUrl)
// 	if err != nil {
// 		t.Error("初始化数据库失败", err)
// 		return
// 	}
// 	oql.AddObject(&Object{
// 		Name: "账簿",
// 		Api:  "person",
// 		Fields: []*Field{
// 			{
// 				Name: "名字",
// 				Api:  "name",
// 				Type: String,
// 			}, {
// 				Name: "年龄",
// 				Api:  "age",
// 				Type: Int,
// 			}, {
// 				Name: "爱好",
// 				Api:  "aih",
// 				Type: NewArrayType(String),
// 			},
// 		},
// 		Comment: "",
// 	})
// 	err = oql.InitObjects(ctx)
// 	if err != nil {
// 		t.Error("初始化对象失败", err)
// 		return
// 	}
// 	res, err := oql.DoCommands(ctx, []Command{
// 		{
// 			Call: "person.insert",
// 			Args: InsertArgs{
// 				Doc: map[string]any{
// 					"name": "小明",
// 					"age":  19,
// 					"aih":  []string{"篮球", "足球"},
// 				},
// 			},
// 			Result: "person1",
// 		},
// 		{
// 			Call: "person.insert",
// 			Args: InsertArgs{
// 				Doc: M{
// 					"name": "小红",
// 					"age":  M{"$formula": "int(person1.age + 10)"},
// 					"aih":  []string{"唱歌"},
// 				},
// 			},
// 			Result: "person2",
// 		},
// 		{
// 			Call: "person.insert",
// 			Args: InsertArgs{
// 				Doc: M{
// 					"name": "小刚",
// 					"age":  M{"$formula": "int(person2.age + 10)"},
// 					"aih":  M{"$formula": "mapToArr([person1, person2], '_id')"},
// 				},
// 			},
// 			Result: "person3",
// 		},
// 		{
// 			Call: "person.findOneById",
// 			Args: M{
// 				"id": M{"$formula": "person3._id"},
// 			},
// 			Result: "last1",
// 		},
// 		{
// 			Call: "person.findOneById",
// 			Args: M{
// 				"id": M{"$formula": "last1._id"},
// 			},
// 			Result: "last12",
// 		},
// 	}, map[string]any{
// 		"person1Name": M{"$formula": "[person1.name, person2.name, person3.name]"},
// 		"person2Name": M{"$formula": "person2.name"},
// 		"person3Name": M{"$formula": "person3.name"},
// 	})
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	t.Log(res)
// }
