fun main(execCtx: *ExecutionContext) -> int64 {
 var ret = 0
 var tvi: TableVectorIterator
 var oids: [16]uint32
 oids[0] = 1
 oids[1] = 2
 oids[2] = 3
 oids[3] = 4
 oids[4] = 5
 oids[5] = 6
 oids[6] = 7
 oids[7] = 8
 oids[8] = 9
 oids[9] = 10
 oids[10] = 11
 oids[11] = 12
 oids[12] = 13
 oids[13] = 14
 oids[14] = 15
 oids[15] = 16
 @tableIterInitBind(&tvi, execCtx, "lineitem", oids)
 for (@tableIterAdvance(&tvi)) {
   var pci = @tableIterGetPCI(&tvi)
   for (; @pciHasNext(pci); @pciAdvance(pci)) {
     ret = ret + 1
   }
   @pciReset(pci)
 }
 @tableIterClose(&tvi)
 return ret
}
