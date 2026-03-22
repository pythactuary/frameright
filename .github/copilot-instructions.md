# .github/copilot-instructions.md

## Project: Schema

### Core Principles
- **Zero type: ignore comments**: Fix root causes in the type system, don't suppress errors
- **No string literals for users**: Users reference columns via properties (e.g., `sales.customer`)
- **Supports multiple DataFrame backends**: Generically typed - can support pandas, or narwhals (and via narwhals other backends like polars, duckdb, etc.)
- **Type safety everywhere**: Full IDE autocomplete, static type checking
- **Type checking should also check column types during static type checking**
- **The methods on the Schema subclasses must be prefixed with the namespace "sf_" to avoid type collisions**

### When Making Changes
- State the goal/constraint first
- Specify acceptable solutions
- Show concrete examples of desired end-user code
- Check that examples are consistent with the principles above
- Run type checks and tests to ensure no regressions
- Keep the codebase clean and maintainable, adhering to the project's coding standards
- Ensure the documentation is updated to reflect the changes, including examples and API references