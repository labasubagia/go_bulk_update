package utils

import "math"

type Pagination[T any] struct {
	Items       []T `json:"items"`
	Total       int `json:"total"`
	CurrentPage int `json:"current_page,omitempty"`
	Limit       int `json:"limit,omitempty"`
	NextPage    int `json:"next_page,omitempty"`
	PrevPage    int `json:"prev_page,omitempty"`
	TotalPage   int `json:"total_page,omitempty"`
}

type Paginate struct {
	Page, Limit int
}

func (p *Paginate) Offset() int {
	return (p.Page - 1) * p.Limit
}

func PaginateData[T any](pageData []T, total int, paginate *Paginate) Pagination[T] {
	page := 1
	limit := total

	if paginate != nil {
		page = paginate.Page
		limit = paginate.Limit
	}

	totalPage := int(math.Ceil(float64(total) / float64(limit)))

	prev := 1
	if page-1 > 0 {
		prev = page - 1
	}

	next := page
	if page+1 <= totalPage {
		next = page + 1
	}

	return Pagination[T]{
		Items:       pageData,
		Total:       total,
		CurrentPage: page,
		Limit:       limit,
		NextPage:    next,
		PrevPage:    prev,
		TotalPage:   totalPage,
	}
}
