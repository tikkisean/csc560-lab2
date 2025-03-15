package godb

import (
	"fmt"
	"sort"
)

type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
	// TODO: some code goes here
	// add additional fields here
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending}, nil

}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// TODO: some code goes here
// HINT: You need to use the Sort function for the implement of Iterator
// Using this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list.

// multiSorter implements the Sort interface, sorting the changes within.
type multiSorter struct {
	data      []Tuple
	orderBy   []Expr
	ascending []bool
}

// Swap is part of sort.Interface.
func (ms *multiSorter) Swap(i, j int) {
	ms.data[i], ms.data[j] = ms.data[j], ms.data[i]
}

// Len is part of sort.Interface.
func (ms *multiSorter) Len() int {
	return len(ms.data)
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (ms *multiSorter) Less(i, j int) bool {
	p, q := &ms.data[i], &ms.data[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.orderBy); k++ {
		orderBy := ms.orderBy[k]
		var cmp orderByState

		if ms.ascending[k] {
			cmp, _ = p.compareField(q, orderBy)
		} else {
			cmp, _ = q.compareField(p, orderBy)
		}

		switch cmp {
		case OrderedLessThan:
			// p < q, so we have a decision.
			return true
		case OrderedGreaterThan:
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	var cmp orderByState
	if ms.ascending[k] {
		cmp, _ = p.compareField(q, ms.orderBy[k])
	} else {
		cmp, _ = q.compareField(p, ms.orderBy[k])
	}

	return cmp == OrderedLessThan
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (ms *multiSorter) Sort(data []Tuple) {
	ms.data = data
	sort.Sort(ms)
}

// OrderedBy returns a Sorter that sorts using the less functions, in order.
// Call its Sort method to sort the data.
func OrderedBy(orderBy []Expr, ascending []bool) *multiSorter {
	return &multiSorter{
		orderBy:   orderBy,
		ascending: ascending,
	}
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// make the sorted stuff here
	sorted := []Tuple{}

	it, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	for {
		tuple, err := it()
		if err != nil {
			return nil, err
		}
		if tuple == nil {
			break
		}
		sorted = append(sorted, *tuple)
	}

	// now do the sorting

	OrderedBy(o.orderBy, o.ascending).Sort(sorted)

	i := 0

	return func() (*Tuple, error) {
		if i >= len(sorted) {
			return nil, nil
		}

		retVal := sorted[i]
		i++
		return &retVal, nil
	}, fmt.Errorf("order_by_op.Iterator not implemented") //replace me
}
