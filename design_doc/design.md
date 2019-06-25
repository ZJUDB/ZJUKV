## 总体设计
### silktree设计
逻辑上，silktree在SSD上维护的是一个BTree leaf list，动态地进行range分区，每个叶子节点就是一个范围。叶子节点里存储的是多个delta, 每个delta是一个sorted run，表示对叶子节点范围内的key的写入，最多存储T个delta，超过之后需要compaction或者split。对BTree leaf list的更新是采用先在内存中buffer足够多的数据，再进行merge的方式。

silktree实际上有两个组件，一个是memtable，另外一个是SSD上的一层delta log。memtable记成`M`, delta log记成`L0`。memtable的durability由一个WAL保证。新delta以append-only的形式写入delta log。当叶子节点发生compaction/split后， log里的一些delta将会invalidate掉，因此需要GC。

为了检索log中的这些leaf deltas，我们对这些leaf建立一个索引层，索引key为该leaf中的最大key，索引项内容包括delta物理位置、block index、filter和访问模式信息等。查询时就可以seek到可能包含search key的leaf节点索引信息。leaf索引较小，一般总能cache在内存中。
### 内存使用量控制
```|M| = |L0| / R```，其中`R`用于控制内存使用量，一般取决于机器内存，30~50比较合适。
### 写放大
主要来源于：WAL，GC，叶子节点的compaction/split。
不过写放大将会比较小：
* 单层log设计本身降低了不少的写放大。
* 根据每个叶子的访问模式(偏向读还是偏向写），采取不同的compaction策略，比如说是写优化或者读优化。
* delta log不需要保证叶子的delta顺序，只需要对stable storage足够友好即可，即顺序写入。它的物理组织和`log-structured file systems`类似，按照以segment为单位切分组织。为了降低GC时候的写放大，segment应该尽可能向bimodal分布靠拢<sup>[1,2]</sup>：一类segment存储的是更新很频繁的叶子节点的delta，另一类segment存储更新很不频繁的叶子节点的delta。为了往这种方向上靠，我们可以根据叶子的访问方式，将访问方式相近的叶子节点的delta写入到相同的segment里。
### 读放大
因为一个leaf中最多允许存储T个delta，决定了读放大。适中的的取值为10-15。
* 点查询：O(1)。
* 小范围扫描：T次IO。
* 假设写入的key均匀分布，为了严格控制每个叶子节点的delta数据量至少为一个storage block大小`U`，不浪费读IO，我们严格控制叶子节点的数量不超过`L = |M| / U`。
* 策略：在merge过程，若叶子节点数量已近达`L`，并且某个leaf的delta数量已超过T，那么采取叶子节点compaction的方式，而不是split。

### 索引所占内存
内存开销主要来自于block index、delta信息和bloom filter。假设数据库含有N条记录，每个storage block能存储B个记录,key的大小为16 bytes，delta位置需要8 bytes。

* block index 开销: 约为N/B * 16 bytes
* delta位置信息开销：最大开销约为L * T * 8 bytes。
* bloom filter开销：false positive rate 1%，每个key需要10个bit。

总开销大概在 N/B * 16 bytes + N * 1.25 bytes + L * T * 8 bytes.

#### 例子
    R=40, T=10, N=1e9,  B=20，U=4KB, key大小为16 bytes，数据总大小D=200GB。
    block index开销：1e9/20 * 16 bytes ~ 0.75GB. 
    delta位置信息开销：L = |M| / U = 5GB/4KB ~ 1.22M个叶子，开销为1.22M * 10 * 8 bytes = 0.09GB.
    bloom filter开销： 1e9 * 10bits ~ 1.16GB. 
    总计1.16 + 0.75 + 0.09 ~ 2GB，占总数据大小的1%。
    再加上memtable的大小，占总数据大小的3.5%。
### 物理存储组织
因为数据以append only的形式写入`Delta Log`，并且需要做GC。我们将一个Log切分成多个较大的`segment`文件，比如每个`segment`的大小为64MB。GC粒度为`segment`。
###### Segment File Format
segment文件由delta组成
```text
[delta 1] 
[delta 2]
...
[delta n]
[fix-sized footer]
```

###### Variable-sized Delta Chunk Format
Delta是一组有序kv pairs, 按block切分，block的大小一般为4KB。`Leaf Identifier`用于GC时，寻找该delta应该属于哪个leaf。
```text
Leaf Identifier Length
Leaf Identifier
[block 1]
[block 2]
...
[block m]
```

### Leaf索引
Leaf Index，需要提供几种功能：
* 根据`search key`，寻找可能包含该key的leaf索引项信息。
* Merge的时候，能有序遍历出所有的leaf索引项。
* Merge时，可以更新一个leaf索引项信息。

初步考虑，将Leaf索引信息存储在leveldb/rocksdb中，因为它们提供了seek/scan等核心功能。
###### Leaf Index Entry Format
```text
max_key_len
max_key
num_deltas
[pos of delta 1]
[pos of delta 2]
...
[pos of delta num_deltas]
[block index 1]
[block index 2]
...
[block index num_deltas]
[filter 1]
[filter 2]
...
[filter num_deltas]
```
##### 引用
1. Mendel Rosenblum and John K. Ousterhout. 1992. *The design and implementation of a log-structured file system*. ACM Trans. Comput. Syst. 10, 1 (February 1992), 26-52.
2. Changwoo Min, Kangnyeon Kim, Hyunjin Cho, Sang-Won Lee, and Young Ik Eom. 2012. *SFS: random write considered harmful in solid state drives*. In Proceedings of the 10th USENIX conference on File and Storage Technologies (FAST'12). USENIX Association, Berkeley, CA, USA, 12-12.