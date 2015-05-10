package com.lp;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;

/**
 * Created by gawain.xiao on 5/9/15.
 */
public class Reader {
    private final ByteBufferPool _bbPool;
    private final Writer _writer;
    private final int _threadNum;
    private final String _inputFileName;


    protected FileChannel _fc = null;
    protected ExecutorService _executor = null;
    protected CountDownLatch _writerCountDown = null;

    public Reader(final ByteBufferPool bbPool, final Writer writer, final int threadNum, final String inputFileName) {
        _bbPool = bbPool;
        _writer = writer;
        _threadNum = threadNum;
        _inputFileName = inputFileName;
    }

    public long start() throws Exception {
        // create executor
        _executor = Executors.newFixedThreadPool(_threadNum);

        // open file
        File file = new File(_inputFileName);
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        _fc = raf.getChannel();

        // create the init block
        BlockInfo prevBlock = new BlockInfo(null);
        prevBlock.setBasicBlockInfo(null, 0, true);
        prevBlock.setAdvanceInfo(0, 0);

        // create reader tasks
        long blockSize = _bbPool.getBlockSize();
        long totBlockNum = (file.length() + blockSize - 1)/blockSize;
        _writerCountDown = new CountDownLatch((int)totBlockNum);

        for(int i=0; i<totBlockNum; i++) {
            long taskBlockSize = Math.min(blockSize, file.length() - i*blockSize);
            Task task = _createReadTask(_executor, _writerCountDown, prevBlock, _fc, i*blockSize, taskBlockSize);
            prevBlock = task.getBlockInfo();
        }

        return totBlockNum;
    }

    public void stop() throws Exception {

        // wait writer to finish all tasks
        _writerCountDown.await();

        if(_executor != null) {
            _executor.shutdown();
            _executor.awaitTermination(3, TimeUnit.SECONDS);
        }

        if(_fc != null) {
            _fc.close();
        }
    }


    public class Task implements Callable<BlockInfo> {
        private final BlockInfo _block; // read result of current block

        private final ByteBuffer _outbb;
        private final ByteBuffer _inbb;

        public Task(final ByteBuffer outbb, final ByteBuffer inbb, BlockInfo prevBlock) {
            _block = new BlockInfo(prevBlock);

            _outbb = outbb;
            _inbb = inbb;
        }

        public BlockInfo getBlockInfo() {
            return _block;
        }

        public BlockInfo call() throws Exception {
            // load block
            _outbb.clear();
            _outbb.put(_inbb);

            // calc the line number
            long numOfCR = 0;
            _outbb.flip().mark();
            byte lastByte = '\0';
            while (_outbb.hasRemaining()) {
                if ((lastByte = _outbb.get()) == '\n') {
                    numOfCR++;
                }
            }

            _outbb.reset();
            _block.setBasicBlockInfo(_outbb, numOfCR, (lastByte == '\n'));
            return _block;
        }
    }

    //-------- internal functions ----
    private Task _createReadTask(final ExecutorService executor,
                                 final CountDownLatch writerCountDown,
                             final BlockInfo prevBlock,
                             final FileChannel fc,
                             final long offset,
                             final long bytes) throws Exception {

        ByteBuffer outbb = _bbPool.allocBuf();
        ByteBuffer inbb = fc.map(FileChannel.MapMode.READ_ONLY, offset, bytes);
        Reader.Task task = new Task(outbb, inbb, prevBlock);
        Future<BlockInfo> future = executor.submit(task);

        _writer.submit(future, writerCountDown);
        return task;
    }

}
