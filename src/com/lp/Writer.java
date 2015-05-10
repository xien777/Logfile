package com.lp;

import com.sun.xml.internal.bind.v2.model.annotation.RuntimeAnnotationReader;

import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;

/**
 * Created by gawain.xiao on 5/9/15.
 */
public class Writer {

    private final ByteBufferPool _bbPool;
    private final int _writerThreads;
    private final String _outputFileName;

    private ExecutorService _executor = null;
    RandomAccessFile _wf = null;

    public Writer(final ByteBufferPool bbPool, final int writerThreads, final String outputFileName) throws Exception {
        _bbPool = bbPool;
        _writerThreads = writerThreads;
        _outputFileName = outputFileName;


    }

    public void start() throws Exception {

        _executor = Executors.newFixedThreadPool(_writerThreads);

        // open write file
        File file = new File(_outputFileName);
        _wf = new RandomAccessFile(file, "rw");
    }

    public void stop() throws Exception {
        if(_wf != null) {
            _wf.close();
        }

        if(_executor != null) {
            _executor.shutdown();
            _executor.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    // submit a task to writer executor
    public void submit(Future<BlockInfo> future, CountDownLatch writerCountDown) {
        _executor.submit(new Task(future, writerCountDown));
    }

    public  class Task implements Runnable {
        private final Future<BlockInfo> _readFuture;
        private final CountDownLatch _writerCountDown;


        public Task(Future<BlockInfo> readFuture, final CountDownLatch writerCountDown) {
            _readFuture = readFuture;
            _writerCountDown = writerCountDown;
        }

        public void run() {
            try {
                // 1. waiting block reading
                BlockInfo block = _readFuture.get();

                // 2. waiting counting the previous block
                BlockInfo prevBlock = block._prevBlock;
                prevBlock.waitAdcanceInfoReady();

                // calculate line number
                long lineCnt = block.numOfCR;
                if (prevBlock.endWithCR) {
                    lineCnt++;
                }
                if (block.endWithCR) {
                    lineCnt--;
                }

                // calculate the write block size
                long writeBytes = block.bb.remaining();
                for (int i = 1; i <= lineCnt; i++) {
                    BigDecimal dec = new BigDecimal(prevBlock.endLineNumber + i);
                    writeBytes = writeBytes + dec.precision() + 2; // "lineNun: "
                }
                block.setAdvanceInfo(prevBlock.endLineNumber + lineCnt, prevBlock.endFileOffset + writeBytes);
                
                // write file
                ByteBuffer inbb = block.bb;
                FileChannel fc = _wf.getChannel();
                MappedByteBuffer outbb = fc.map(FileChannel.MapMode.READ_WRITE, prevBlock.endFileOffset, writeBytes);

                long lineNo = prevBlock.endLineNumber;
                int inOffset = 0;
                while (inbb.hasRemaining()) {
                    int lineBytes = 0;
                    do {
                        lineBytes++;
                    }
                    while (inbb.get() != '\n' && inbb.hasRemaining());

                    if (inOffset != 0 || prevBlock.endWithCR) {
                        byte[] lineNoAry = ("" + lineNo + ": ").getBytes();
                        lineNo++;
                        outbb.put(lineNoAry);
                    }
                    outbb.put(inbb.array(), inOffset, lineBytes);
                    inOffset += lineBytes;
                }

                _bbPool.releaseBuf(block.bb);
                _writerCountDown.countDown();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

}
