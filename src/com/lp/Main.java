package com.lp;

public class Main {

    public static void main(String[] args) {

        String inputFileName = "big.log";
        String outputFileName = inputFileName + "-";

        final int bufSize = 8 * 1024 * 1024;
        final int bufNum = 16;
        final int readerThreads = 4;
        final int writerThreads = 4;

        try {
            // create ByteBuffer pool
            ByteBufferPool bbPool = new ByteBufferPool(bufSize, bufNum);
            Writer writer = new Writer(bbPool, writerThreads, outputFileName);
            Reader reader = new Reader(bbPool, writer, readerThreads, inputFileName);

            writer.start();
            reader.start();

            reader.stop();
            writer.stop();
        }
        catch (Exception e) {

        }
   }
}
