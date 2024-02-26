package objectql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/gogf/gf/v2/errors/gerror"
)

func TestUpdateableFormula(t *testing.T) {
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
					Name:          "年龄",
					Api:           "age",
					Type:          Int,
					Updateable:    "admin",
					UpdateableMsg: "管理员才能修改",
				},
				{
					Name: "管理员",
					Api:  "admin",
					Type: Bool,
				},
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		_, err := oql.DoCommands(ctx, []Command{
			{
				Call: "student.insert",
				Args: M{
					"doc": M{
						"name":  "老陈",
						"admin": false,
					},
				},
				Fields: []string{
					"_id",
				},
				Result: "student1",
			},
			{
				Call: "student.updateById",
				Args: M{
					"id": M{"$formula": "student1._id"},
					"doc": M{
						"age": 10,
					},
				},
				Result: "student1_1",
			},
		})
		if err == nil {
			return errors.New("except error but got nil")
		}
		if !strings.Contains(err.Error(), "管理员才能修改") {
			return gerror.Newf("except error '管理员才能修改', but got %s", err.Error())
		}

		_, err = oql.DoCommands(ctx, []Command{
			{
				Call: "student.insert",
				Args: M{
					"doc": M{
						"name":  "老陈",
						"admin": false,
					},
				},
				Fields: []string{
					"_id",
				},
				Result: "student1",
			},
			{
				Call: "student.updateById",
				Args: M{
					"id": M{"$formula": "student1._id"},
					"doc": M{
						"age":   10,
						"admin": true,
					},
				},
				Result: "student1_1",
			},
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}

func TestUpdateableHandle(t *testing.T) {
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
					Updateable: &FieldUpdateableHandle{
						Fields: []string{"admin"},
						Handle: func(ctx context.Context, cur *Var) error {
							if !cur.Bool("admin") {
								return errors.New("管理员才能修改")
							}
							return nil
						},
					},
				},
				{
					Name: "管理员",
					Api:  "admin",
					Type: Bool,
				},
			},
			Comment: "",
		},
	}
	err := testTransaction(list, func(ctx context.Context, oql *Objectql) error {
		_, err := oql.DoCommands(ctx, []Command{
			{
				Call: "student.insert",
				Args: M{
					"doc": M{
						"name":  "老陈",
						"admin": false,
					},
				},
				Fields: []string{
					"_id",
				},
				Result: "student1",
			},
			{
				Call: "student.updateById",
				Args: M{
					"id": M{"$formula": "student1._id"},
					"doc": M{
						"age": 10,
					},
				},
				Result: "student1_1",
			},
		})
		if err == nil {
			return errors.New("except error but got nil")
		}
		if !strings.Contains(err.Error(), "管理员才能修改") {
			return gerror.Newf("except error '管理员才能修改', but got %s", err.Error())
		}

		_, err = oql.DoCommands(ctx, []Command{
			{
				Call: "student.insert",
				Args: M{
					"doc": M{
						"name":  "老陈",
						"admin": false,
					},
				},
				Fields: []string{
					"_id",
				},
				Result: "student1",
			},
			{
				Call: "student.updateById",
				Args: M{
					"id": M{"$formula": "student1._id"},
					"doc": M{
						"age":   10,
						"admin": true,
					},
				},
				Result: "student1_1",
			},
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Error(gerror.Stack(err))
		return
	}
}
