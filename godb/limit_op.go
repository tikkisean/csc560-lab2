package godb

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	cnt := int64(0)
	limit, err := l.limitTups.EvalExpr(nil)
	if err != nil {
		return nil, err
	}

	it, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		tup, err := it()
		if err != nil {
			return nil, err
		}
		if tup == nil || limit.EvalPred(IntField{cnt}, OpEq) {
			return nil, nil
		}

		cnt++
		return tup, nil

	}, nil
}
