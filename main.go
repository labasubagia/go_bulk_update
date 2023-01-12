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

type User struct {
	ID      int    `db:"id"`
	Name    string `db:"name"`
	Age     int    `db:"age"`
	Address string `db:"address"`
}

type Opt struct {
	start, size     int
	method          string
	worker          int
	updateBatchSize int
	clearAtEnd      bool
}

func ExecUser(opt Opt) error {
	fieldSize, err := utils.CountField(User{})
	if err != nil {
		panic(err)
	}

	data := []User{}
	table := "user"
	keyEdits := []string{"id"}

	mySQL, err := db.NewSQL("root:root@(localhost:3307)/test_db", opt.worker, opt.updateBatchSize)
	if err != nil {
		return err
	}

	// Clear
	if err := mySQL.EmptyTable(table); err != nil {
		return err
	}

	// Create
	for i := opt.start; i < opt.start+opt.size; i++ {
		data = append(data, User{
			ID:      i,
			Age:     10,
			Name:    fmt.Sprintf("Name_%v", i),
			Address: fmt.Sprintf("Addr_%v", i),
		})
	}
	payload, err := utils.StructsToMaps(data, "db")
	if err != nil {
		return err
	}
	if err := mySQL.CreateBulk(table, payload, fieldSize); err != nil {
		return err
	}

	// Update
	for index := range data {
		number := index + opt.start
		data[index].Age = number
		data[index].Name = fmt.Sprintf("NAME_%v", number)
		data[index].Address = fmt.Sprintf("ADDR_%v", number)
	}

	startTime := time.Now()
	payload, err = utils.StructsToMaps(data, "db")
	if err != nil {
		return err
	}
	method := reflect.ValueOf(mySQL).MethodByName(opt.method)
	params := []reflect.Value{}
	for _, v := range []any{table, payload, keyEdits, fieldSize} {
		params = append(params, reflect.ValueOf(v))
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

	log.Println("Start")
	defer log.Println("Finish")

	var wg sync.WaitGroup
	methods := []string{
		"UpdateBulkManual",
		"UpdateBulk",
	}
	for _, method := range methods {
		opt := Opt{
			method:          method,
			start:           start,
			size:            size,
			worker:          worker,
			updateBatchSize: updateBatchSize,
			clearAtEnd:      clearAtEnd,
		}
		start += size

		wg.Add(1)
		go func(opt Opt) {
			defer wg.Done()
			if err := ExecUser(opt); err != nil {
				log.Println(err)
			}
		}(opt)
	}
	wg.Wait()

}
