struct output_struct {
  colA: Integer
  colB: Integer
}

// SELECT * FROM test_1 WHERE colA=500;
fun main(execCtx: *ExecutionContext) -> int64 {
  // output variable
  var out : *output_struct
  // Index iterator
  var index : IndexIterator
  @indexIteratorConstructBind(&index, "test_ns", "test_1", "index_1", execCtx)
  @indexIteratorPerformInit(&index)
  @indexIteratorSetKeyInt(&index, 0, @intToSql(500))
  // Attribute to indicate which iterator to use
  for (@indexIteratorScanKey(&index); @indexIteratorAdvance(&index);) {
    out = @ptrCast(*output_struct, @outputAlloc(execCtx))
    out.colA = @indexIteratorGetInt(&index, 0)
    out.colB = @indexIteratorGetInt(&index, 1)
    @outputAdvance(execCtx)
  }
  // Finalize output
  @indexIteratorFree(&index)
  @outputFinalize(execCtx)
  return 0
}