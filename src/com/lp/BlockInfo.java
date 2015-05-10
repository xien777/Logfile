package com.lp;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by gawain.xiao on 5/10/15.
 */
public class BlockInfo {

    public final BlockInfo _prevBlock; // read result of previous block

    // basic block information
    public ByteBuffer bb;
    public long numOfCR;
    public boolean endWithCR;

    // advance block info
    public CountDownLatch advInfoReady = new CountDownLatch(1);
    public long endLineNumber;
    public long endFileOffset;

    public BlockInfo(BlockInfo blockInfo) {
        _prevBlock = blockInfo;
    }

    public void setBasicBlockInfo(final ByteBuffer bb, long numOfCR, boolean endWithCR) {
        this.bb = bb;
        this.numOfCR = numOfCR;
        this.endWithCR = endWithCR;
    }

    public void setAdvanceInfo(long endLineNumber, long endFileOffset) {
        this.endLineNumber = endLineNumber;
        this.endFileOffset = endFileOffset;

        advInfoReady.countDown();
    }

    public void waitAdcanceInfoReady() throws Exception {
        advInfoReady.await();
    }

}
