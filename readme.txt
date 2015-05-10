1. The big file is divided into blocks with same block size(except the last block). The processing unit is block.
2. The reader and writer executor is separated, so they can be adjusted to adapt the disk IO properties.
3. For read tasks, they are totally irrelative, so the blocks can be loaded into memory in parallel.
4. For a write task, it depends on two conditions:
  3.1 the read executor have loaded the block into memory
  3.2 the end line number of previous block is known
