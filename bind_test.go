package objectql

import (
	"context"
	"fmt"
	"testing"

	"github.com/gogf/gf/v2/frame/g"
)

var listenCount = 0

type bindListenObjecct struct{}

func (b *bindListenObjecct) InsertBefore(ctx context.Context, doc *Var) error {
	listenCount += 1
	return nil
}

func (b *bindListenObjecct) InsertAfter(ctx context.Context, id string, doc *Var) error {
	listenCount += 10
	return nil
}

func (b *bindListenObjecct) UpdateBefore(ctx context.Context, id string, doc *Var) error {
	listenCount += 100
	return nil
}

func (b *bindListenObjecct) UpdateAfter(ctx context.Context, id string, doc *Var) error {
	listenCount += 1000
	return nil
}

func (b *bindListenObjecct) DeleteBefore(ctx context.Context, id string) error {
	listenCount += 10000
	return nil
}

func (b *bindListenObjecct) DeleteAfter(ctx context.Context, id string) error {
	listenCount += 100000
	return nil
}

func TestBindListen(t *testing.T) {
	listenCount = 0
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
		Bind: &bindListenObjecct{},
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
		t.Error("初始化对象失败", err)
		return
	}

	res, err := oql.Insert(ctx, "student", InsertOptions{
		Doc: map[string]any{
			"name": "小明",
			"age":  18,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = oql.UpdateById(ctx, "student", UpdateByIdOptions{
		ID: res.String("_id"),
		Doc: map[string]any{
			"name": "小明",
			"age":  18,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = oql.DeleteById(ctx, "student", DeleteByIdOptions{
		ID: res.String("_id"),
	})
	if err != nil {
		t.Error(err)
		return
	}

	// t.Log(count)
	if listenCount != 111111 {
		t.Errorf("except 11111 but got %d", listenCount)
		return
	}
}

var methodCount = 0

type getHomeAddrReq struct {
	g.Meta `kind:"query"`
	Number int
}

type setHomeAddrReq struct {
	g.Meta `kind:"mutation"`
	Number int
	Addr   string
}

type bindMethodObjecct struct{}

func (b *bindMethodObjecct) GetHomeAddr(ctx context.Context, req *getHomeAddrReq) (string, error) {
	methodCount += 1
	return "Quan Zhou", nil
}

func (b *bindMethodObjecct) SetHomeAddr(ctx context.Context, req *setHomeAddrReq) (bool, error) {
	methodCount += 10
	return true, nil
}

func TestBindMethod(t *testing.T) {
	listenCount = 0
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
		Bind: &bindMethodObjecct{},
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
		t.Error("初始化对象失败", err)
		return
	}

	_, err = oql.Query(ctx, "student", "getHomeAddr", map[string]any{
		"number": 101,
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = oql.Mutation(ctx, "student", "setHomeAddr", map[string]any{
		"number": 101,
		"addr":   "BeiJing",
	})
	if err != nil {
		t.Error(err)
		return
	}

	if methodCount != 11 {
		t.Errorf("except 11 but got %d", methodCount)
		return
	}
}

type getObjectInfosReq struct {
	g.Meta `kind:"query"`
	Number int
}

type testBindStructArrayStruct struct{}

func (t *testBindStructArrayStruct) GetObjectInfos(ctx context.Context, req *getObjectInfosReq) ([]resultStruct, error) {
	return nil, nil
}

type resultStruct struct {
	Name string
}

func TestBindStructArray(t *testing.T) {
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
		Bind: &testBindStructArrayStruct{},
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
		t.Error("初始化对象失败", err)
		return
	}
}

type testOneResultHandleObjectSetNameReq struct {
	g.Meta `kind:"mutation"`
}

type TestOneResultHandleObject struct{}

func (to *TestOneResultHandleObject) SetName1(ctx context.Context, req *testOneResultHandleObjectSetNameReq) error {
	fmt.Println("SetName1")
	return nil
}

func (to *TestOneResultHandleObject) SetName2(ctx context.Context, req *testOneResultHandleObjectSetNameReq) error {
	fmt.Println("SetName2")
	return nil
}

func (to *TestOneResultHandleObject) SetName3(ctx context.Context, req *testOneResultHandleObjectSetNameReq) error {
	fmt.Println("SetName3")
	return nil
}

func (to *TestOneResultHandleObject) SetName4(ctx context.Context, req *testOneResultHandleObjectSetNameReq) error {
	fmt.Println("SetName4")
	return nil
}

func (to *TestOneResultHandleObject) SetName5(ctx context.Context, req *testOneResultHandleObjectSetNameReq) error {
	fmt.Println("SetName5")
	return nil
}

func TestOneResultHandle(t *testing.T) {
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
		Bind: &TestOneResultHandleObject{},
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
		t.Error("初始化对象失败", err)
		return
	}

	res, err := oql.Call(ctx, "student", "setName2", nil)
	if err != nil {
		t.Error(err)
		return
	}
	if res.ToBool() != true {
		t.Errorf("except true but got %v", res)
		return
	}
}
