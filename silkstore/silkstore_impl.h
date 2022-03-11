//
// Created by zxjcarrot on 2019-07-05.
//

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef SILKSTORE_DB_IMPL_H_
#define SILKSTORE_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "db/write_batch_internal.h"
#include "leaf_store.h"
#include "segment.h"
#include "nvm/nvmemtable.h"
#include "nvm/nvmmanager.h"
#include "nvm/nvmleafindex.h"
namespace leveldb {
namespace silkstore {

class GroupedSegmentAppender;

class SilkStore : public DB {
public:
    SilkStore(const Options &options, const std::string &dbname);

    virtual ~SilkStore();

    // Implementations of the DB interface
    virtual Status Put(const WriteOptions &, const Slice &key, const Slice &value);

    virtual Status Delete(const WriteOptions &, const Slice &key);

    virtual Status Write(const WriteOptions &options, WriteBatch *updates);

    virtual Status Get(const ReadOptions &options,
                       const Slice &key,
                       std::string *value);

    virtual Iterator *NewIterator(const ReadOptions &);

    virtual const Snapshot *GetSnapshot();

    virtual void ReleaseSnapshot(const Snapshot *snapshot);

    virtual bool GetProperty(const Slice &property, std::string *value);

    virtual void GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {}

    virtual void CompactRange(const Slice *begin, const Slice *end) {}

    // Extra methods (for testing) that are not in the public DB interface

    // Compact any files in the named level that overlap [*begin,*end]
    void TEST_CompactRange(int level, const Slice *begin, const Slice *end);

    // Force current memtable contents to be compacted.
    Status TEST_CompactMemTable();

    // Return an internal iterator over the current state of the database.
    // The keys of this iterator are internal keys (see format.h).
    // The returned iterator should be deleted when no longer needed.
    Iterator *TEST_NewInternalIterator();

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    int64_t TEST_MaxNextLevelOverlappingBytes();

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.
    void RecordReadSample(Slice key);


    Status OpenIndex(const Options &index_options);

    void BackgroundCompaction();

    Status CopyMinirunRun(Slice leaf_max_key, LeafIndexEntry &index_entry, uint32_t run_idx_in_index_entry,
                          SegmentBuilder *seg_builder, WriteBatch & leaf_index_wb);

    Status GarbageCollectSegment(Segment *seg, GroupedSegmentAppender &appender, WriteBatch & leaf_index_wb);

    int GarbageCollect();

    std::string SegmentsSpaceUtilityHistogram();

    void Destroy();

private:

    friend class DB;

    struct CompactionState;
    struct Writer;

    // Constant after construction
    Env *const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    //const Options options_;  // options_.comparator == &internal_comparator_
    Options options_;
    const bool owns_info_log_;
    const bool owns_cache_;
    const std::string dbname_;

    Options leaf_index_options_;  // options_.comparator == &internal_comparator_

    // Leaf index
    DB *leaf_index_;

    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    FileLock *db_lock_;

    port::Mutex GCMutex;

    //port::Mutex LeafMutex;

    // State below is protected by mutex_
    port::Mutex mutex_;
    port::AtomicPointer shutting_down_;
    port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
    NvmemTable *mem_;
    NvmemTable *imm_ GUARDED_BY(mutex_);  // Memtable being compacted
    NvmManager *nvm_manager_;

    port::AtomicPointer has_imm_;       // So bg thread can detect non-null imm_
    WritableFile *logfile_;
    uint64_t logfile_number_ GUARDED_BY(mutex_);
    log::Writer *log_;
    uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.
    SequenceNumber max_sequence_ GUARDED_BY(mutex_);
    size_t memtable_capacity_ GUARDED_BY(mutex_);;
    size_t allowed_num_leaves = 0;
    size_t num_leaves = 0;
    SegmentManager *segment_manager_;
    // Queue of writers.
    std::deque<Writer *> writers_ GUARDED_BY(mutex_);
    WriteBatch *tmp_batch_ GUARDED_BY(mutex_);

    SnapshotList snapshots_ GUARDED_BY(mutex_);

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

    // Has a background compaction been scheduled or is running?
    bool background_compaction_scheduled_ GUARDED_BY(mutex_);

    // =====================================================================
    port::Mutex leaf_op_mutex_;
    bool background_leaf_optimization_scheduled_ GUARDED_BY(leaf_op_mutex_);
    port::CondVar background_leaf_op_finished_signal_ GUARDED_BY(leaf_op_mutex_);
    // =====================================================================

    std::function<void()> leaf_optimization_func_;
    // Information for a manual compaction
    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey *begin;   // null means beginning of key range
        const InternalKey *end;     // null means end of key range
        InternalKey tmp_storage;    // Used to keep track of compaction progress
    };
    ManualCompaction *manual_compaction_ GUARDED_BY(mutex_);

    // Have we encountered a background error in paranoid mode?
    Status bg_error_ GUARDED_BY(mutex_);

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;

        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

        void Add(const CompactionStats &c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }
    };


    // No copying allowed
    SilkStore(const SilkStore &);

    void operator=(const SilkStore &);

    const Comparator *user_comparator() const {
        return internal_comparator_.user_comparator();
    }

    Status MakeRoomForWrite(bool force /* compact even if there is room? */)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Recover the descriptor from persistent storage.  May do a significant
    // amount of work to recover recently logged updates.  Any changes to
    // be made to the descriptor are added to *edit.
    Status Recover() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status RecoverNvmemtable(uint64_t log_number, SequenceNumber *max_sequence) EXCLUSIVE_LOCKS_REQUIRED(mutex_);;

    Status RecoverLogFile(uint64_t log_number, SequenceNumber *max_sequence) EXCLUSIVE_LOCKS_REQUIRED(mutex_);;

    WriteBatch *BuildBatchGroup(Writer **last_writer)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void MaybeScheduleCompaction();

    Status DoCompactionWork(WriteBatch &leaf_index_wb);

    Status OptimizeLeaf();

    Status MakeRoomInLeafLayer(bool force = false);

    static void BGWork(void *db);

    void BackgroundCall();

    Status InvalidateLeafRuns(const LeafIndexEntry &leaf_index_entry, size_t start_run, size_t end_run);

    LeafIndexEntry
    CompactLeaf(SegmentBuilder *seg_builder, uint32_t seg_no, const LeafIndexEntry &leaf_index_entry, Status &s,
                std::string *buf, uint32_t start_minirun_no, uint32_t end_minirun_no,
                const Snapshot *leaf_index_snap = nullptr);

    std::pair<uint32_t, uint32_t> ChooseLeafCompactionRunRange(const LeafIndexEntry &leaf_index_entry);


    // silkstore stuff
    LeafStore *leaf_store_ = nullptr;
    LeafStatStore stat_store_;

    struct MergeStats {
        size_t bytes_written = 0;
        size_t bytes_read = 0;

        size_t gc_bytes_written = 0;
        size_t gc_bytes_read = 0;
        size_t gc_bytes_read_unopt = 0;

        // # miniruns queried in leaf_index_ for validness during GC.
        size_t gc_miniruns_queried;
        // # miniruns in total checked during GC.
        // gc_miniruns_total - gc_miniruns_queried => # miniruns that are skipped by
        size_t gc_miniruns_total;

        void Add(size_t read, size_t written) {
            bytes_read += read;
            bytes_written += written;
        }

        void AddGCUnoptStats(size_t read) {
            gc_bytes_read_unopt += read;
        }

        void AddGCStats(size_t read, size_t written) {
            gc_bytes_written += written;
            gc_bytes_read += read;
        }

        void AddGCMiniRunStats(size_t miniruns_queried, size_t miniruns_total) {
            gc_miniruns_queried += miniruns_queried;
            gc_miniruns_total += miniruns_total;
        }

        size_t time_spent_compaction = 0;
        size_t time_spent_gc = 0;

        void AddTimeCompaction(size_t t) {
            time_spent_compaction += t;
        }

        void AddTimeGC(size_t t) {
            time_spent_gc += t;
        }
    } stats_;

    // parallel compaction
    // Maintains state for each sub-compaction
    struct SubCompaction {
        std::string *start_, *end_;
        std::string *value_;
        
        SubCompaction():start_(nullptr), 
                        end_(nullptr), 
                        value_(nullptr) {}
        SubCompaction(std::string *start, std::string *end, std::string *value): start_(start),
                                                end_(end),
                                                value_(value) {}
    };

    struct CompactSubTaskState {
        size_t read_ = 0;
        size_t written_ = 0;
        int32_t leaf_change_num_ = 0;
        Status s_;
        WriteBatch leaf_index_wb_;
    };

    size_t compact_num_threads_ = 2;

    // Stores the boundaries for each subcompaction
    // subcompaction states are stored in order of increasing key-range
    std::vector<SubCompaction> sub_compact_tasks_;

    std::vector<CompactSubTaskState> compact_subtask_states_;

    // the max key of each leaf_index_entry
    std::vector<std::string> boundries_;
    // the value of each leaf_index_entry
    std::vector<std::string> leaf_values_;

    // It adds the max key and value of each leaf_index_entry
    // Then it is uesd to divides immutable to sum groups.
    void GenSubcompactionBoundaries();

    // prepare subtasks for multiple threads
    void PrepareCompactionTasks();

    // Launch threads for each subcompaction and wait for them to finish.
    void RunCompactionTasks();

    // Assign tasks to threads
    void ProcessCompactionSubTasks(int tid);

    // Iterate through immutable and compact the kv-pairs.
    void ProcessKeyValueCompaction(SubCompaction &sub_compact, CompactSubTaskState &state, GroupedSegmentAppender &grouped_segment_appender);

    Status FinishCompactionTasks();

    // parallel make room for leaf layer
    // mainly responsible for split leaf
    struct SingleLeaf {
        std::string max_key_;
        std::string value_;

        SingleLeaf():max_key_(""),
                        value_("") {}
        SingleLeaf(std::string max_key, std::string value): max_key_(max_key), value_(value) {}
    };

    struct SplitLeafTaskState {
        size_t read_ = 0;
        size_t written_ = 0;
        int32_t leaf_change_num_ = 0;
        Status s_;
        WriteBatch leaf_index_wb_;
    };

    size_t split_leaf_num_threads_ = 4;

    // the leafs need split
    std::vector<SingleLeaf> leafs_need_split;
    std::vector<SplitLeafTaskState> split_subtask_states_;
    // prepare the leafs need split
    void PrepareLeafsNeedSplit(bool force);
    // launch threads for each subcompaction and wait for them to finish.
    void RunSplitLeafTasks();

    // Assign tasks to threads
    void ProcessSplitLeafSubTasks(int tid);

    // do split leaf work
    void ProcessOneLeaf(SingleLeaf &leaf,
                        SplitLeafTaskState &state,
                        GroupedSegmentAppender &grouped_segment_appender);
    // finish all tasks and aggregate
    Status FinishSplitLeafTasks();

};

Status DestroyDB(const std::string &dbname, const Options &options);

}  // namespace silkstore
}  // namespace leveldb

#endif  // SILKSTORE_DB_IMPL_H_
