package main

import (
	"go_update_bulk/db"
	"go_update_bulk/generator"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type BulkUpdateOption struct {
	generator  generator.Generator
	method     string
	clearAtEnd bool
	keyEdits   []string
}

func ExecBulkUpdate(sql db.SQL, opt BulkUpdateOption) error {

	table := opt.generator.Table()
	fieldSize := opt.generator.FieldCount()

	// Clear
	if err := sql.EmptyTable(table); err != nil {
		return err
	}

	// Create
	if err := sql.CreateBulk(table, opt.generator.GetCreate(), fieldSize); err != nil {
		return err
	}

	// Update
	// func fn(table string, data []map[string]any, keyEdits []string, fieldSize int) error
	startTime := time.Now()
	method := reflect.ValueOf(sql).MethodByName(opt.method)
	params := []reflect.Value{}

	for _, param := range []any{table, opt.generator.GetUpdate(), opt.keyEdits, fieldSize} {
		params = append(params, reflect.ValueOf(param))
	}
	result := method.Call(params)
	if len(result) > 0 {
		if err := result[0].Interface(); err != nil {
			return err.(error)
		}
	}
	elapsed := time.Since(startTime)
	log.Printf("%s with %d data took %fs\n", opt.method, opt.generator.TotalData(), elapsed.Seconds())

	// Clear
	if opt.clearAtEnd {
		if err := sql.EmptyTable(table); err != nil {
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

	worker := 2
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

	// Connect database
	sql, err := db.NewSQL(dataSourceName, worker, updateBatchSize)
	if err != nil {
		panic(err)
	}
	defer sql.Close()

	var wg sync.WaitGroup
	methods := []string{
		"UpdateSequential",
		"UpdateParallel",
		"UpdateBulk",
	}

	for _, method := range methods {
		gen := generator.NewGenerator(start, size, generator.NewUserDump(), "db", true)
		opt := BulkUpdateOption{
			generator:  gen,
			method:     method,
			keyEdits:   keyEdits,
			clearAtEnd: clearAtEnd,
		}
		start += size

		wg.Add(1)
		go func(opt BulkUpdateOption) {
			defer wg.Done()
			if err := ExecBulkUpdate(sql, opt); err != nil {
				log.Println(err)
			}
		}(opt)
	}
	wg.Wait()
}
