package godb

import (
	"fmt"
)

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
	//add additional fields here
	// TODO: some code goes here
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	return &Project{selectFields: selectFields, outputNames: outputNames, child: child, distinct: distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fields := []FieldType{}

	if len(p.outputNames) != len(p.selectFields) {
		fmt.Errorf("The length of the outputNames and selectFields arrays are not the same.")
	}

	for i, val := range p.selectFields {
		fieldType := val.GetExprType()
		fieldType.Fname = p.outputNames[i]
		fields = append(fields, fieldType)
	}

	return &TupleDesc{fields}

}

func contains(s []Tuple, t Tuple) bool {
	for _, seen := range s {
		if seen.equals(&t) {
			return true
		}
	}
	return false
}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// make this not a slice
	seen := []Tuple{}
	fields := []FieldType{}

	for _, val := range p.selectFields {
		fieldType := val.GetExprType()
		fields = append(fields, fieldType)
	}

	it, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {

		for {
			tup, err := it()
			if err != nil {
				return nil, err
			}
			if tup == nil {
				return nil, nil
			}

			outTup, err := tup.project(fields)
			if err != nil {
				return nil, err
			}

			if contains(seen, *outTup) {
				continue
			} else {
				seenDescFields := make([]FieldType, len(outTup.Desc.Fields))
				copy(seenDescFields, outTup.Desc.Fields)

				seen = append(seen, Tuple{
					TupleDesc{seenDescFields}, outTup.Fields, outTup.Rid})

				// reset the names using the outputNames
				for i := range outTup.Desc.Fields {
					outTup.Desc.Fields[i].Fname = p.outputNames[i]
				}

				return outTup, nil
			}
		}
	}, nil
}
