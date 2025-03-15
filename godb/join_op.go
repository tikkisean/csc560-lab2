package godb

type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
}

// Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [TupleDesc.merge].
func (hj *EqualityJoin) Descriptor() *TupleDesc {
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())
}

// Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	leftIter, err := (*joinOp.left).Iterator(tid)
	if err != nil {
		return nil, err
	}

	rightIter, err := (*joinOp.right).Iterator(tid)
	if err != nil {
		return nil, err
	}

	leftTuple, err := leftIter()
	if err != nil {
		return nil, err
	}
	if leftTuple == nil {
		return nil, nil
	}

	return func() (*Tuple, error) {
		for {
			leftVal, err := joinOp.leftField.EvalExpr(leftTuple)
			if err != nil {
				return nil, err
			}

			for {
				rightTuple, err := rightIter()
				if err != nil {
					return nil, err
				}
				if rightTuple == nil {
					leftTuple, err = leftIter()
					if err != nil {
						return nil, err
					}
					if leftTuple == nil {
						return nil, nil
					}

					rightIter, err = (*joinOp.right).Iterator(tid)
					if err != nil {
						return nil, err
					}

					break
				}

				rightVal, err := joinOp.rightField.EvalExpr(rightTuple)
				if err != nil {
					return nil, err
				}

				if leftVal.EvalPred(rightVal, OpEq) {
					return joinTuples(leftTuple, rightTuple), nil
				}
			}
		}
	}, nil
}
