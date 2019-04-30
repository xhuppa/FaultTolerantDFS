package edu.gmu.cs475.test;

import edu.gmu.cs475.internal.TestingClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    @Rule
    public Timeout globalTimeout = new Timeout(60000);

    @Test
    public void testCantWriteWhileDisconnectedFromZK() throws Exception {

        TestingClient c1 = newClient("Leader (C1)");
        blockUntilLeader(c1);
        TestingClient c2 = newClient("Follower (C2)");
        TestingClient c3 = newClient("Follower (C3)");
        blockUntilMemberJoins(c2);
        blockUntilMemberJoins(c3);

        String k1 = "testCantWriteWhileDisconnectedFromZK-key-1";//getNewKey();

        String v1 = getNewValue();
        //String v2 = getNewValue();
        //String v3 = getNewValue();

        //c2.setValue(k1, v1);

       /* Assert.assertEquals(v1, c2.getValue(k1));
        Assert.assertEquals(v1, c3.getValue(k1));
        Assert.assertEquals(v1, c1.getValue(k1));*/
        System.out.println("K1 "+k1);
        System.out.println("V1 "+v1);
        System.out.println("C1 "+c1);
        System.out.println("C2 "+c2);
        System.out.println("C3 "+c3);
        c1.setValue(k1, v1);
        c2.setValue(k1, v1);

        //setKeyAndRead(false, k1, v1, c1, c2, c3);

        c1.invalidateKey(k1);

        //c2.invalidateKey("testCantWriteWhileDisconnectedFromZK-key-1");
        //c3.invalidateKey("testCantWriteWhileDisconnectedFromZK-key-1");

        //setKeyAndRead(false, k1, v1, c1, c2);

        assertInvalidateCalled(c1, k1);

        String k2 = getNewKey();
        c3.setValue(k2, v1);

        c1.resumeAccessToZK();





    }

    @Test
    public void testReadingWhileLeaderNotInZKAndNoOtherNodes() throws Exception {

    }

    @Test
    public void testWritingWhileReaderWithCacheDisconnectedFromZK() throws Exception {

    }

}
