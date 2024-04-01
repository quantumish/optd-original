// Required properties are interesting characteristics of an expression that
// impact its layout, presentation, or location, but not its logical content.
// Examples include row order, column naming, and data distribution (physical
// location of data ranges). Physical properties exist outside of the relational
// algebra, and arise from both the SQL query itself (e.g. the non-relational
// ORDER BY operator) and by the selection of specific implementations during
// optimization (e.g. a merge join requires the inputs to be sorted in a
// particular order).
//
// Required properties are derived top-to-bottom - there is a required physical
// property on the root, and each expression can require physical properties on
// one or more of its operands. When an expression is optimized, it is always
// with respect to a particular set of required physical properties. The goal
// is to find the lowest cost expression that provides those properties while
// still remaining logically equivalent.
struct Required {
	// Ordering specifies the sort order of result rows. Rows can be sorted by
	// one or more columns, each of which can be sorted in either ascending or
	// descending order. If Ordering is not defined, then no particular ordering
	// is required or provided.
	Ordering props.OrderingChoice

}