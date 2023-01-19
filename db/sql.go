package db

import (
	"context"
	"errors"
	"fmt"
	"go_update_bulk/utils"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/semaphore"
)

type SQL interface {
	DB() *sqlx.DB
	CreateBulk(table string, data []map[string]any, fieldSize int) error
	UpdateBulk(table string, data []map[string]any, keyEdits []string, fieldSize int) error
	UpdateParallel(table string, data []map[string]any, keyEdits []string, fieldSize int) error
	UpdateSequential(table string, data []map[string]any, keyEdits []string, fieldSize int) error
	Update(table string, data, condition map[string]any) error
	Delete(table string, condition map[string]any) error
	EmptyTable(table string) error
	Close() error
}

type sql struct {
	db         *sqlx.DB
	workerSize int
	batchSize  int
}

func NewSQL(dataSourceName string, workerSize, batchSize int) (SQL, error) {
	if dataSourceName == "" {
		return nil, errors.New("data source name is empty")
	}
	if workerSize <= 0 {
		return nil, errors.New("worker size min 1")
	}
	if batchSize <= 0 {
		return nil, errors.New("batch size min 1")
	}

	db, err := sqlx.Connect("mysql", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed connect database: %w", err)
	}
	sql := sql{
		db:         db,
		workerSize: workerSize,
		batchSize:  batchSize,
	}
	return &sql, nil
}

func (s *sql) DB() *sqlx.DB {
	return s.db
}

func (s *sql) CreateBulk(table string, data []map[string]any, fieldSize int) error {
	if table == "" {
		return errors.New("table is empty")
	}
	if len(data) == 0 {
		return errors.New("data is empty")
	}
	if fieldSize <= 0 {
		return errors.New("field size minimum 1")
	}
	query, _, err := utils.CreateQuery(table, data[0])
	if err != nil {
		return fmt.Errorf("failed build query %w", err)
	}

	ctx := context.Background()
	sem := semaphore.NewWeighted(int64(s.workerSize))

	size := len(data)
	pageSize := utils.BulkMaxDataSize(size, fieldSize*size)
	paged := utils.PagedData(data, pageSize)

	errors := make(chan error, len(paged))

	create := func(pageNumber int, data []map[string]any) error {
		if _, err = s.db.NamedExec(query, data); err != nil {
			return fmt.Errorf("error when create page %d: %w", pageNumber, err)
		}
		return nil
	}

	for index, page := range paged {
		pageNumber := index + 1
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("error acquire semaphore on page %d: %w", pageNumber, err)
		}
		go func(pageNumber int, data []map[string]any) {
			defer sem.Release(1)
			if err := create(pageNumber, data); err != nil {
				errors <- err
			}
		}(pageNumber, page)
	}
	if err := sem.Acquire(ctx, int64(s.workerSize)); err != nil {
		return fmt.Errorf("error wait semaphore: %w", err)
	}

	close(errors)
	for err := range errors {
		return err
	}
	return nil
}

func (s *sql) UpdateBulk(table string, data []map[string]any, keyEdits []string, fieldSize int) error {
	if table == "" {
		return errors.New("table is empty")
	}
	if len(data) == 0 {
		return errors.New("data is empty")
	}
	if len(keyEdits) == 0 {
		return errors.New("key edits is empty")
	}
	if fieldSize <= 0 {
		return errors.New("field size minimum 1")
	}

	ctx := context.Background()
	sem := semaphore.NewWeighted(int64(s.workerSize))
	size := len(data)

	totalField := utils.BulkUpdateEstimateTotalField(len(data), fieldSize, len(keyEdits))

	pageSize := s.batchSize
	calculatedPageSize := utils.BulkMaxDataSize(size, totalField)
	if calculatedPageSize < pageSize {
		pageSize = calculatedPageSize
	}

	paged := utils.PagedData(data, pageSize)
	errors := make(chan error, len(paged))

	update := func(pageNumber int, data []map[string]any, keyEdit []string) error {
		query, binds, err := utils.BulkUpdateQuery(table, data, keyEdit)
		if err != nil {
			return fmt.Errorf("failed to build query %d: %w", pageNumber, err)
		}
		if _, err := s.db.NamedExec(query, binds); err != nil {
			return fmt.Errorf("error when update page %d: %w", pageNumber, err)
		}
		return nil
	}

	for index, page := range paged {
		pageNumber := index + 1
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("error acquire semaphore on page %d: %w", pageNumber, err)
		}
		go func(pageNumber int, data []map[string]any) {
			defer sem.Release(1)
			if err := update(pageNumber, data, keyEdits); err != nil {
				errors <- err
			}
		}(pageNumber, page)
	}
	if err := sem.Acquire(ctx, int64(s.workerSize)); err != nil {
		return fmt.Errorf("error wait semaphore: %w", err)
	}

	close(errors)
	for err := range errors {
		return err
	}

	return nil
}

func (s *sql) UpdateParallel(table string, data []map[string]any, keyEdits []string, fieldSize int) error {
	if table == "" {
		return errors.New("table is empty")
	}
	if len(data) == 0 {
		return errors.New("data is empty")
	}
	if len(keyEdits) == 0 {
		return errors.New("key edits is empty")
	}
	if fieldSize <= 0 {
		return errors.New("field size minimum 1")
	}

	ctx := context.Background()
	sem := semaphore.NewWeighted(int64(s.workerSize))
	errors := make(chan error, len(data))

	update := func(dataNumber int, data map[string]any) error {
		condition := map[string]any{}
		for _, key := range keyEdits {
			value, ok := data[key]
			if !ok {
				return fmt.Errorf("data %d not have '%s' property", dataNumber, key)
			}
			condition[key] = value
			delete(data, key)
		}
		if err := s.Update(table, data, condition); err != nil {
			return fmt.Errorf("error when update page %d: %w", dataNumber, err)
		}
		return nil
	}

	for index, item := range data {
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("error acquire semaphore: %w", err)
		}
		go func(dataNumber int, data map[string]any) {
			defer sem.Release(1)
			if err := update(dataNumber, data); err != nil {
				errors <- err
			}
		}(index+1, item)
	}
	if err := sem.Acquire(ctx, int64(s.workerSize)); err != nil {
		return fmt.Errorf("error wait semaphore: %w", err)
	}

	close(errors)
	for err := range errors {
		return err
	}

	return nil
}

func (s *sql) UpdateSequential(table string, data []map[string]any, keyEdits []string, fieldSize int) error {
	if table == "" {
		return errors.New("table is empty")
	}
	if len(data) == 0 {
		return errors.New("data is empty")
	}
	if len(keyEdits) == 0 {
		return errors.New("key edits is empty")
	}
	if fieldSize <= 0 {
		return errors.New("field size minimum 1")
	}

	update := func(dataNumber int, data map[string]any) error {
		condition := map[string]any{}
		for _, key := range keyEdits {
			value, ok := data[key]
			if !ok {
				return fmt.Errorf("data %d not have '%s' property", dataNumber, key)
			}
			condition[key] = value
			delete(data, key)
		}
		if err := s.Update(table, data, condition); err != nil {
			return fmt.Errorf("error when update page %d: %w", dataNumber, err)
		}
		return nil
	}

	for index, item := range data {
		if err := update(index+1, item); err != nil {
			return err
		}
	}

	return nil
}

func (s *sql) Update(table string, data, condition map[string]any) error {
	if table == "" {
		return errors.New("table is empty")
	}
	if len(data) == 0 {
		return errors.New("payload is empty")
	}
	if len(condition) == 0 {
		return errors.New("condition is empty")
	}

	query, binds, err := utils.UpdateQuery(table, data, condition)
	if err != nil {
		return fmt.Errorf("failed build query: %w", err)
	}
	query, args, err := sqlx.Named(query, binds)
	if err != nil {
		return fmt.Errorf("failed bind named: %w", err)
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return fmt.Errorf("failed bindVar: %w", err)
	}

	_, err = s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed update: %w", err)
	}

	return nil
}

func (s *sql) Delete(table string, condition map[string]any) error {
	if table == "" {
		return errors.New("table is empty")
	}
	if len(condition) == 0 {
		return errors.New("condition is empty")
	}

	query, binds, err := utils.DeleteQuery(table, condition)
	if err != nil {
		return fmt.Errorf("failed build query: %w", err)
	}
	query, args, err := sqlx.Named(query, binds)
	if err != nil {
		return fmt.Errorf("failed bind named: %w", err)
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return fmt.Errorf("failed bindVar: %w", err)
	}

	_, err = s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed delete: %w", err)
	}

	return nil
}

func (s *sql) EmptyTable(table string) error {
	if table == "" {
		return errors.New("table is empty")
	}
	query := fmt.Sprintf("DELETE FROM %s", table)
	if _, err := s.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (s *sql) Close() error {
	return s.db.Close()
}
