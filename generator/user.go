package generator

import (
	"fmt"
	"time"
)

type User struct {
	ID        *int       `db:"id"`
	Name      *string    `db:"name"`
	Age       *int       `db:"age"`
	Address   *string    `db:"address"`
	UpdatedAt *time.Time `db:"updated_at"`
}

func NewUserDump() GenerateDump[User] {
	return &User{}
}

func (u *User) Table() string {
	return "user"
}

func (u *User) Primary() string {
	return "id"
}

func (u *User) Current() User {
	return *u
}

func (u *User) DumpCreate(no int) User {
	name := fmt.Sprintf("Create Name %d", no)
	address := fmt.Sprintf("Create Address %d", no)
	return User{
		ID:      &no,
		Age:     &no,
		Name:    &name,
		Address: &address,
	}
}

func (u *User) DumpUpdate(no int) User {
	name := fmt.Sprintf("Edited Name %d", no)
	address := fmt.Sprintf("Edited Address %d", no)
	now := time.Now()
	return User{
		ID:        &no,
		Age:       &no,
		Name:      &name,
		Address:   &address,
		UpdatedAt: &now,
	}
}
