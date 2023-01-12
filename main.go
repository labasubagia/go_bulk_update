package main

import (
	"fmt"
	"go_update_bulk/db"
	"go_update_bulk/utils"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Generator interface {
	Table() string
	FieldCount() int
	GetCreate() []map[string]any
	GetUpdate() []map[string]any
}

type User struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Address string `db:"address"`
}

type userGenerator struct {
	tag        string
	dataCreate []User
	dataUpdate []User
}

func NewUserGenerator(start, size int) Generator {
	end := start + size

	dataCreate := make([]User, 0, size)
	dataUpdate := make([]User, 0, size)
	for i := start; i < end; i++ {
		dataCreate = append(dataCreate, User{
			ID:      i,
			Age:     10,
			Name:    fmt.Sprintf("Name_%v", i),
			Address: fmt.Sprintf("Addr_%v", i),
		})
		dataUpdate = append(dataUpdate, User{
			ID:      i,
			Age:     i,
			Name:    fmt.Sprintf("Edited_Name_%v", i),
			Address: fmt.Sprintf("Edited_Addr_%v", i),
		})
	}

	return &userGenerator{
		dataCreate: dataCreate,
		dataUpdate: dataUpdate,
		tag:        "db",
	}
}

func (g *userGenerator) Table() string {
	return "user"
}

func (g *userGenerator) FieldCount() int {
	count, _ := utils.CountField(User{})
	return count
}

func (g *userGenerator) GetCreate() []map[string]any {
	data, _ := utils.StructsToMaps(g.dataCreate, g.tag)
	return data
}

func (g *userGenerator) GetUpdate() []map[string]any {
	data, _ := utils.StructsToMaps(g.dataUpdate, g.tag)
	return data
}

type Opt struct {
	generator       Generator
	dataSourceName  string
	start, size     int
	method          string
	worker          int
	updateBatchSize int
	clearAtEnd      bool
	keyEdits        []string
}

func Exec(opt Opt) error {

	table := opt.generator.Table()
	fieldSize := opt.generator.FieldCount()

	mySQL, err := db.NewSQL(opt.dataSourceName, opt.worker, opt.updateBatchSize)
	if err != nil {
		return err
	}

	// Clear
	if err := mySQL.EmptyTable(table); err != nil {
		return err
	}

	// Create
	if err := mySQL.CreateBulk(table, opt.generator.GetCreate(), fieldSize); err != nil {
		return err
	}

	// Update
	// func fn(table string, data []map[string]any, keyEdits []string, fieldSize int) error
	startTime := time.Now()
	method := reflect.ValueOf(mySQL).MethodByName(opt.method)
	params := []reflect.Value{}
	for _, param := range []any{table, opt.generator.GetUpdate(), opt.keyEdits, fieldSize} {
		params = append(params, reflect.ValueOf(param))
	}
	result := method.Call(params)
	if len(result) > 0 {
		if err := result[0].Interface(); err != nil {
			return fmt.Errorf("%v", err)
		}
	}
	elapsed := time.Since(startTime)
	log.Printf("%v with %v data took %vs\n", opt.method, opt.size, elapsed.Seconds())

	// Clear
	if opt.clearAtEnd {
		if err := mySQL.EmptyTable(table); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	args := os.Args

	start := 1

	size := 1000
	if len(args) > 1 {
		if n, err := strconv.Atoi(os.Args[1]); err == nil {
			size = n
		}
	}

	worker := 1
	if len(args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			worker = n
		}
	}

	updateBatchSize := 100
	if len(args) > 3 {
		if n, err := strconv.Atoi(os.Args[3]); err == nil {
			updateBatchSize = n
		}
	}

	clearAtEnd := false
	dataSourceName := "root:root@(localhost:3307)/test_db"
	keyEdits := []string{"id"}

	log.Println("Start")
	defer log.Println("Finish")

	var wg sync.WaitGroup
	methods := []string{
		"UpdateBulkManual",
		"UpdateBulk",
	}
	for _, method := range methods {
		opt := Opt{
			generator:       NewUserGenerator(start, size),
			dataSourceName:  dataSourceName,
			method:          method,
			start:           start,
			size:            size,
			worker:          worker,
			updateBatchSize: updateBatchSize,
			keyEdits:        keyEdits,
			clearAtEnd:      clearAtEnd,
		}
		start += size

		wg.Add(1)
		go func(opt Opt) {
			defer wg.Done()
			if err := Exec(opt); err != nil {
				log.Println(err)
			}
		}(opt)
	}
	wg.Wait()

}
