package generator

import (
	"fmt"
)

type User struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Address string `db:"address"`
}

func NewUserDump() GenerateDump[User] {
	return &User{}
}

func (u *User) Table() string {
	return "user"
}

func (u *User) Current() User {
	return *u
}

func (u *User) DumpCreate(no int) User {
	return User{
		ID:      no,
		Age:     no,
		Name:    fmt.Sprintf("Create Name %d", no),
		Address: fmt.Sprintf("Create Address %d", no),
	}
}

func (u *User) DumpUpdate(no int) User {
	return User{
		ID:      no,
		Age:     no,
		Name:    fmt.Sprintf("Update Name %d", no),
		Address: fmt.Sprintf("Update Address %d", no),
	}
}
