package com.lp;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;

/**
 * Created by gawain.xiao on 5/9/15.
 */
public class ByteBufferPool {
    private final int _blockSize;
    private final BlockingQueue<ByteBuffer> _queue;

    public ByteBufferPool(final int blockSize, final int blockNum) throws Exception {
        _blockSize = blockSize;

        _queue = new ArrayBlockingQueue<ByteBuffer>(blockNum);
        for(int i=0; i<blockNum; i++) {
            ByteBuffer bb = ByteBuffer.allocate(_blockSize);
            _queue.put(bb);
        }
    }

    public ByteBuffer allocBuf() throws Exception {
        return _queue.take();
    }

    public void releaseBuf(ByteBuffer bb) throws Exception {
        _queue.put(bb);
    }

    public int getBlockSize() {
        return _blockSize;
    }
}
