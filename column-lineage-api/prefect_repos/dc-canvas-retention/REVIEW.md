- Avoid booleans in function signatures. [why this is bad](https://docs.astral.sh/ruff/rules/boolean-default-value-positional-argument/#why-is-this-bad)
  - The `include_active_only` parameter is an example. 
- Its often more efficient to fetch more data than you think you need. For instance, we can get all the view definitions up front vs querying each one separately.
  - View definition
  - The view name (rather than using `common_canvas_next`)
- Avoid passing around Settings objects. If your private query function expects Settings, it's difficult to test. Is also difficult to decouple changes to the Settings object from the query function.
  - Instead, just pass the necessary parameters when calling the function.
- The flow of this code is difficult to follow. 
  - Rather than a function which maps a function to a list, which is in turn potentially passed to another function, try to flatten the code.
- Use the ``@task`` decorator sparingly, if at all. 
  - It encourages bad practices like mentioned above.
  - Anything with threads (like snowflake-connector) will require careful handling, with little benefit.
  - We end up calling .get_engine(), .begin() for each iteration, when a single connection could be used for all iterations.
- If mapping a function to a list where the function requires a db connection, open the transaction outside of loop, and pass the connection to the function.
  - ```python
    with engine.begin() as conn:
        for item in items:
            my_function(conn, item)
    ```
  - This allows any failures to rollback the transaction.
- Use keyword arguments vs positional arguments.
  - This makes refactoring easier
- Follow naming conventions for sql statements:
  - Is this a query? Use `query_` prefix
  - Is this parsing a query into a pydantic model? Use `get_` prefix
  - Is this updating a record? Use `make_` prefix (for 'making the sql statement')
- Invert the if statement for readability
  - ```python
    if not some_flag:
       do_this()
       some_call()
       model.save()
    return
    
    # vs
    if some_flag:
        return
    do_this()
    some_call()
    model.save()
    ``` 
  