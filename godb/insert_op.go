package godb

type InsertOp struct {
	file      DBFile
	op        Operator
	completed bool
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{file: insertFile, op: child, completed: false}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constructor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return func() (*Tuple, error) {
		count := int64(0)
		if !iop.completed {
			// do all the insertion stuff
			it, err := iop.op.Iterator(tid)
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

				if err := iop.file.insertTuple(tuple, tid); err != nil {
					return nil, err
				} else {
					count++
				}
			}

			iop.completed = true
		}

		return &Tuple{Desc: *iop.Descriptor(), Fields: []DBValue{IntField{count}}}, nil
	}, nil
}
