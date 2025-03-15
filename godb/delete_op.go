package godb

type DeleteOp struct {
	file      DBFile
	op        Operator
	completed bool
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{file: deleteFile, op: child, completed: false}
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return func() (*Tuple, error) {
		count := int64(0)
		if !dop.completed {
			// do all the insertion stuff
			it, err := dop.op.Iterator(tid)
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

				if err := dop.file.deleteTuple(tuple, tid); err != nil {
					return nil, err
				} else {
					count++
				}
			}

			dop.completed = true
		}

		return &Tuple{Desc: *dop.Descriptor(), Fields: []DBValue{IntField{count}}}, nil
	}, nil
}
