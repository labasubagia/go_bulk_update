package db

import (
	"context"
	"errors"
	"fmt"
	"go_update_bulk/utils"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/semaphore"
)

type SQL struct {
	db         *sqlx.DB
	workerSize int
	batchSize  int
}

func NewSQL(dataSourceName string, workerSize, batchSize int) (*SQL, error) {
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
	sql := SQL{
		db:         db,
		workerSize: workerSize,
		batchSize:  batchSize,
	}
	return &sql, nil
}

func (s *SQL) CreateBulk(table string, data []map[string]any, fieldSize int) error {
	if table == "" {
		return errors.New("table is empty")
	}
	if len(data) == 0 {
		return errors.New("data is empty")
	}
	if fieldSize <= 0 {
		return errors.New("field size minimum 1")
	}
	query, _, err := utils.BulkCreateQuery(table, data[0])
	if err != nil {
		return fmt.Errorf("failed build query %w", err)
	}

	ctx := context.Background()
	sem := semaphore.NewWeighted(int64(s.workerSize))

	size := len(data)
	pageSize := utils.BulkUpdateMaxDataSize(size, fieldSize*size)
	paged := utils.PagedData(data, pageSize)

	errors := make(chan error, len(paged))

	create := func(pageNumber int, data []map[string]any) error {
		if _, err = s.db.NamedExec(query, data); err != nil {
			return fmt.Errorf("error when create page %v: %w", pageNumber, err)
		}
		return nil
	}

	for index, page := range paged {
		pageNumber := index + 1
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("error acquire semaphore on page %v: %w", pageNumber, err)
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

func (s *SQL) UpdateBulk(table string, data []map[string]any, keyEdits []string, fieldSize int) error {
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
	calculatedPageSize := utils.BulkUpdateMaxDataSize(size, totalField)
	if calculatedPageSize < pageSize {
		pageSize = calculatedPageSize
	}

	paged := utils.PagedData(data, pageSize)
	errors := make(chan error, len(paged))

	update := func(pageNumber int, data []map[string]any, keyEdit []string) error {
		query, binds, err := utils.BulkUpdateQuery(table, data, keyEdit)
		if err != nil {
			return fmt.Errorf("failed to build query %v: %w", pageNumber, err)
		}
		if _, err := s.db.NamedExec(query, binds); err != nil {
			return fmt.Errorf("error when update page %v: %w", pageNumber, err)
		}
		return nil
	}

	for index, page := range paged {
		pageNumber := index + 1
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("error acquire semaphore on page %v: %w", pageNumber, err)
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

func (s *SQL) UpdateBulkManual(table string, data []map[string]any, keyEdits []string, fieldSize int) error {
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
				return fmt.Errorf("data %v not have '%v' property", dataNumber, key)
			}
			condition[key] = value
			delete(data, key)
		}
		if err := s.Update(table, data, condition); err != nil {
			return fmt.Errorf("error when update page %v: %w", dataNumber, err)
		}
		return nil
	}

	for index, item := range data {
		if err := sem.Acquire(ctx, 1); err != nil {
			return fmt.Errorf("error acquire semaphore: %w", err)
		}
		go func(pageNumber int, data map[string]any) {
			defer sem.Release(1)
			if err := update(pageNumber, data); err != nil {
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

func (s *SQL) Update(table string, data, condition map[string]any) error {
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

func (s *SQL) EmptyTable(table string) error {
	if table == "" {
		return errors.New("table is empty")
	}
	query := fmt.Sprintf("DELETE FROM %s", table)
	if _, err := s.db.Exec(query); err != nil {
		return err
	}
	return nil
}
