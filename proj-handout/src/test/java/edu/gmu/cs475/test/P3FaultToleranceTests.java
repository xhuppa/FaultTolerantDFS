package edu.gmu.cs475.test;

public class P3FaultToleranceTests extends Base475Test {

    /**
     * P3FaultToleranceTests.testCantWriteWhileDisconnectedFromZK:
     * 170 Expected rejoined old leader to be able to read new value expected:
     * <...nnectedFromZK-value-[2]> but was:<...nnectedFromZK-value-[1]>
     *
     * P3FaultToleranceTests.testReadingWhileLeaderNotInZKAndNoOtherNodes:
     * 114 Expected rejoined old leader to be able to read new value expected:
     * <...dNoOtherNodes-value-[2]> but was:<...dNoOtherNodes-value-[1]>
     *
     * P3FaultToleranceTests.testWritingWhileReaderWithCacheDisconnectedFromZK:
     * 264 After reconnecting client c2 and a succesful write,
     * expected c2 to see new value expected:
     * <...nnectedFromZK-value-[2]> but was:<...nnectedFromZK-value-[1]>
     */


}
