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
	// Presentation specifies the naming, membership (including duplicates),
	// and order of result columns. If Presentation is not defined, then no
	// particular column presentation is required or provided.
	Presentation Presentation

	// Ordering specifies the sort order of result rows. Rows can be sorted by
	// one or more columns, each of which can be sorted in either ascending or
	// descending order. If Ordering is not defined, then no particular ordering
	// is required or provided.
	Ordering props.OrderingChoice

	// LimitHint specifies a "soft limit" to the number of result rows that may
	// be required of the expression. If requested, an expression will still need
	// to return all result rows, but it can be optimized based on the assumption
	// that only the hinted number of rows will be needed.
	// A LimitHint of 0 indicates "no limit". The LimitHint is an intermediate
	// float64 representation, and can be converted to an integer number of rows
	// using LimitHintInt64.
	LimitHint float64

	// Distribution specifies the physical distribution of result rows. This is
	// defined as the set of regions that may contain result rows. If
	// Distribution is not defined, then no particular distribution is required.
	// Currently, the only operator in a plan tree that has a required
	// distribution is the root, since data must always be returned to the gateway
	// region.
	Distribution Distribution
}