fun main(execCtx: *ExecutionContext) -> int {
 var tvi : TableVectorIterator
 @tableIterConstructBind(&tvi, "lineitem", execCtx, "li")
 @tableIterAddColBind(&tvi, "li", "l_returnflag")
 @tableIterAddColBind(&tvi, "li", "l_linestatus")
 @tableIterAddColBind(&tvi, "li", "l_quantity")
 @tableIterAddColBind(&tvi, "li", "l_extendedprice")
 @tableIterAddColBind(&tvi, "li", "l_discount")
 @tableIterAddColBind(&tvi, "li", "l_tax")
 @tableIterAddColBind(&tvi, "li", "l_shipdate")
 @tableIterPerformInitBind(&tvi, "li")
 for (@tableIterAdvance(&tvi)) {
   var pci = @tableIterGetPCI(&tvi)
 }
 @tableIterClose(&tvi)
 return 0
}
