# Partial Exploration

When the plan space is very large, optd_og will generate a sub-optimal plan at first, and then use the runtime information to continue the plan space search next time the same query (or a similar query) is being optimized. This is partial exploration.

Developers can pass `partial_explore_iter` and `partial_explore_space` to the optimizer options to specify how large the optimizer will expand each time `step_optimize_rel` is invoked. To use partial exploration, developers should not clear the internal state of the optimizer across different runs.
