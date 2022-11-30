//
// Created by zxjcarrot on 2019-07-05.
//
#include <cmath>
#include <functional>
#include <memory>
#include <queue>
#include <thread>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "leveldb/write_batch.h"
#include "table/merger.h"
#include "util/mutexlock.h"

#include "util/histogram.h"
#include "silkstore/silkstore_impl.h"
#include "silkstore/silkstore_iter.h"
#include "silkstore/util.h"

int runs_searched = 0;
int runs_hit_counts = 0;
int runs_miss_counts = 0;
int bloom_filter_counts = 0;
namespace leveldb {

Status DB::OpenSilkStore(const Options& options, const std::string& name,
                         DB** dbptr) {
  Options silkstore_options = options;
  silkstore_options.env = Env::NewPosixEnv();
  *dbptr = nullptr;
  silkstore::SilkStore* store =
      new silkstore::SilkStore(silkstore_options, name);
  Status s = store->Recover();
  if (s.ok()) {
    *dbptr = store;
    return s;
  } else {
    delete store;
    return s;
  }
}

namespace silkstore {

const std::string kCURRENTFilename = "CURRENT";

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

static Options SanitizeOptions(const std::string& dbname,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + 10, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  return result;
}

SilkStore::SilkStore(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      leaf_index_(nullptr),
      db_lock_(nullptr),
      shutting_down_(nullptr),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      max_sequence_(0),
      memtable_capacity_(options_.write_buffer_size),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      leaf_optimization_func_([]() {}),
      background_leaf_op_finished_signal_(&leaf_op_mutex_),
      background_leaf_optimization_scheduled_(false),
      manual_compaction_(nullptr) {
  nvm_manager_ =
      new NvmManager(raw_options.nvmemtable_file, raw_options.nvmemtable_size);
  has_imm_.Release_Store(nullptr);
}

SilkStore::~SilkStore() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  leaf_op_mutex_.Lock();
  while (background_leaf_optimization_scheduled_) {
    background_leaf_op_finished_signal_.Wait();
  }
  leaf_op_mutex_.Unlock();

  // Delete leaf index
  delete leaf_index_;
  leaf_index_ = nullptr;

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  // delete versions_
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  // delete table_cache_
  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status SilkStore::OpenIndex(const Options& index_options) {
  assert(leaf_index_ == nullptr);
  Status s =
      NvmLeafIndex::OpenNvmLeafIndex(index_options, dbname_, &leaf_index_);

  auto it = leaf_index_->NewIterator(ReadOptions{});
  DeferCode c([it]() { delete it; });
  int cnt = 0;
  std::map<int, int> counts;
  it->SeekToFirst();
  while (it->Valid()) {
    LeafIndexEntry index_entry(it->value());
    int nums = index_entry.GetNumMiniRuns();
    stat_store_.NewLeaf(it->key().ToString(), nums);
    ++cnt;
    counts[nums]++;
    it->Next();
  }
  std::cout << "NvmLeafIndex NumMiniRuns\n";
  for (auto it : counts) {
    std::cout << "NumMiniRuns: " << it.first << " count " << it.second << "\n";
  }
  return s;
}

static std::string MakeFileName(const std::string& dbname, uint64_t number,
                                const char* prefix, const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/%s%06llu.%s", prefix,
           static_cast<unsigned long long>(number), suffix);
  return dbname + buf;
}

static std::string LogFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "", "log");
}

static std::string CurrentFilename(const std::string& dbname) {
  return dbname + "/" + kCURRENTFilename;
}

Status SilkStore::RecoverNvmemtable(uint64_t log_number,
                                    SequenceNumber* max_sequence) {
  // std::cout << "RecoverNvmemtable\n";
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read the log records
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  NvmemTable* mem = nullptr;
  if (reader.ReadRecord(&record, &scratch)) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      fprintf(stderr, "RecoverMemtable Corruption log record too small\n");
    }
  }
  // split record into string numbers
  std::string rec = record.ToString();
  std::string delimiter = ",";
  std::vector<size_t> records;
  size_t pos;
  while ((pos = rec.find(delimiter)) != std::string::npos) {
    records.push_back(std::stoll(rec.substr(0, pos)));
    rec.erase(0, pos + delimiter.length());
  }

  if (records.size() < 3 || records.size() % 2 != 1) {
    fprintf(stderr, "RecoverMemtable Corruption\n");
    std::cout << "records.size:" << records.size() << " ";
    for (auto it : records) {
      std::cout << it << " ";
    }
    std::cout << " \n";
    return Status::NotSupported("RecoverMemtable");
  }
  // recovery nvm_manager_
  nvm_manager_->recovery(records);
  // recovery memtable;
  int len = records.size() - 1;
  mem_ =
      new NvmemTable(internal_comparator_, nullptr,
                     nvm_manager_->reallocate(records[len - 1], records[len]));
  SequenceNumber last_seq;
  mem_->Recovery(last_seq);
  mem_->Ref();

  if (last_seq > *max_sequence) {
    *max_sequence = last_seq;
  }
  // recovery imm memtable
  if (records.size() > 3) {
    for (int i = len - 2; i >= 1; i -= 2) {
      NvmemTable* imm =
          new NvmemTable(internal_comparator_, nullptr,
                         nvm_manager_->reallocate(records[i - 1], records[i]));
      imm->Recovery(last_seq);
      if (last_seq > *max_sequence) {
        *max_sequence = last_seq;
      }
      imm->Ref();
    }
  }
  delete file;
  return Status::OK();
}

Status SilkStore::Recover() {
  MutexLock g(&mutex_);
  this->leaf_index_options_.create_if_missing = true;
  this->leaf_index_options_.filter_policy = NewBloomFilterPolicy(10);
  this->leaf_index_options_.block_cache = NewLRUCache(8 << 26);
  this->leaf_index_options_.compression = kNoCompression;
  Status s = OpenIndex(this->leaf_index_options_);
  if (!s.ok()) return s;
  // Open segment manager
  s = SegmentManager::OpenManager(this->options_, dbname_, &segment_manager_,
                                  std::bind(&SilkStore::GarbageCollect, this));
  if (!s.ok()) return s;
  s = LeafStore::Open(segment_manager_, leaf_index_, options_,
                      internal_comparator_.user_comparator(), &leaf_store_);
  if (!s.ok()) return s;
  std::string current_content;
  s = ReadFileToString(env_, CurrentFilename(dbname_), &current_content);
  if (s.IsNotFound()) {
    // new db
    mem_ = new NvmemTable(internal_comparator_, nullptr,
                          nvm_manager_->allocate(100 * MB));
    mem_->Ref();
    SequenceNumber log_start_seq_num = max_sequence_ = 1;
    WritableFile* lfile = nullptr;
    s = env_->NewWritableFile(LogFileName(dbname_, log_start_seq_num), &lfile);
    if (!s.ok()) return s;
    logfile_ = lfile;
    log_ = new log::Writer(logfile_);
    std::string temp_current = dbname_ + "/" + "CURRENT_temp";
    s = WriteStringToFile(env_, std::to_string(log_start_seq_num),
                          temp_current);
    if (!s.ok()) return s;
    s = env_->RenameFile(temp_current, CurrentFilename(dbname_));
    // modified by yunxiao to record nvm info
    Status status = log_->AddRecord(nvm_manager_->getNvmInfo());
    if (!status.ok()) {
      printf("logfile_->Sync() Error \n");
      return Status::NotSupported("logfile_->Sync() Error");
    }
    status = logfile_->Sync();
    if (!status.ok()) {
      printf("logfile_->Sync() Error \n");
      return Status::NotSupported("logfile_->Sync() Error");
    }
  } else {
    Iterator* it = leaf_index_->NewIterator(ReadOptions{});
    DeferCode c([it]() { delete it; });
    it->SeekToFirst();
    num_leaves = 0;
    while (it->Valid()) {
      ++num_leaves;
      it->Next();
    }
    allowed_num_leaves = num_leaves;
    size_t new_memtable_capacity_ =
        (allowed_num_leaves + 1) *
        options_.storage_block_size;  // todo 怎么设置合理
    memtable_capacity_ = new_memtable_capacity_ > memtable_capacity_
                             ? new_memtable_capacity_
                             : memtable_capacity_;
    SequenceNumber log_start_seq_num = std::stoi(current_content);
    s = RecoverNvmemtable(log_start_seq_num, &max_sequence_);
  }
  if (!s.ok()) return s;

  leaf_optimization_func_ = [this]() {
    this->OptimizeLeaf();
    background_leaf_optimization_scheduled_ = false;
    background_leaf_op_finished_signal_.SignalAll();
    leaf_op_mutex_.Unlock();
    if (shutting_down_.Acquire_Load()) {
      // No more background work when shutting down.
    } else {
      leaf_op_mutex_.Lock();
      background_leaf_optimization_scheduled_ = true;
      env_->ScheduleDelayedTask(leaf_optimization_func_,
                                LeafStatStore::read_interval_in_micros);
    }
  };

  leaf_op_mutex_.Lock();
  background_leaf_optimization_scheduled_ = true;
  env_->ScheduleDelayedTask(leaf_optimization_func_,
                            LeafStatStore::read_interval_in_micros);
  return s;
}

Status SilkStore::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

// Convenience methods
Status SilkStore::Put(const WriteOptions& o, const Slice& key,
                      const Slice& val) {
  return DB::Put(o, key, val);
}

Status SilkStore::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

namespace {

struct IterState {
  port::Mutex* const mu;
  NvmemTable* const mem GUARDED_BY(mu);
  NvmemTable* const imm GUARDED_BY(mu);
  IterState(port::Mutex* mutex, NvmemTable* mem, NvmemTable* imm)
      : mu(mutex), mem(mem), imm(imm) {}
};

static void SilkStoreNewIteratorCleanup(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->mu->Unlock();
  delete state;
}
}  // anonymous namespace

const Snapshot* SilkStore::GetSnapshot() {
  MutexLock l(&mutex_);
  return leaf_index_->GetSnapshot();
}

void SilkStore::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  return leaf_index_->ReleaseSnapshot(snapshot);
}

Iterator* SilkStore::NewIterator(const ReadOptions& ropts) {
  MutexLock l(&mutex_);
  SequenceNumber seqno =
      ropts.snapshot
          ? dynamic_cast<const SnapshotImpl*>(ropts.snapshot)->sequence_number()
          : max_sequence_;
  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  list.push_back(leaf_store_->NewIterator(ropts));
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  IterState* cleanup = new IterState(&mutex_, mem_, imm_);
  internal_iter->RegisterCleanup(SilkStoreNewIteratorCleanup, cleanup, nullptr);
  return leveldb::silkstore::NewDBIterator(
      internal_comparator_.user_comparator(), internal_iter, seqno);
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status SilkStore::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    size_t memtbl_size = mem_->ApproximateMemoryUsage();
    if (!force && (memtbl_size <= memtable_capacity_)) {
      break;
    } else if (imm_ != nullptr) {
      Log(options_.info_log,
          "Current memtable full;Compaction ongoing; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      uint64_t new_log_number = max_sequence_;
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      size_t old_memtable_capacity = memtable_capacity_;
      size_t new_memtable_capacity =
          (memtable_capacity_ + segment_manager_->ApproximateSize()) /
          options_.memtbl_to_L0_ratio;
      new_memtable_capacity =
          std::min(options_.max_memtbl_capacity,
                   std::max(options_.write_buffer_size, new_memtable_capacity));
      Log(options_.info_log, "new memtable capacity %lu\n",
          new_memtable_capacity);
      memtable_capacity_ = new_memtable_capacity;

      // std::cout << "new_memtable_capacity: " << memtable_capacity_
      // /(1024*1024)<< "MB\n";

      // Fix memtable's capacity_
      /*  size_t new_memtable_capacity = 1024ul*1024ul*1024ul;
       memtable_capacity_ = new_memtable_capacity;
       size_t old_memtable_capacity = memtable_capacity_; */

      allowed_num_leaves = std::ceil(new_memtable_capacity /
                                     (options_.storage_block_size + 0.0));
      DynamicFilter* dynamic_filter = nullptr;
      if (options_.use_memtable_dynamic_filter) {
        size_t imm_num_entries = imm_->NumEntries();
        size_t new_memtable_capacity_num_entries =
            imm_num_entries *
            std::ceil(new_memtable_capacity / (old_memtable_capacity + 0.0));
        assert(new_memtable_capacity_num_entries);
        dynamic_filter =
            NewDynamicFilterBloom(new_memtable_capacity_num_entries,
                                  options_.memtable_dynamic_filter_fp_rate);
      }
      // mem_ = new MemTable(internal_comparator_, dynamic_filter);
      // TODO Opt nvm's alllocate
      mem_ = new NvmemTable(
          internal_comparator_, dynamic_filter,
          nvm_manager_->allocate(new_memtable_capacity + 4 * MB));
      Status status = log_->AddRecord(nvm_manager_->getNvmInfo());
      if (!status.ok()) {
        printf("logfile_->Sync() Error \n");
        assert(false);
      }
      status = logfile_->Sync();
      if (!status.ok()) {
        printf("logfile_->Sync() Error \n");
        assert(false);
      }
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

void SilkStore::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }
  background_compaction_scheduled_ = false;
  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void SilkStore::BGWork(void* db) {
  reinterpret_cast<SilkStore*>(db)->BackgroundCall();
}

void SilkStore::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&SilkStore::BGWork, this);
  }
}

// Information kept for every waiting writer
struct SilkStore::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
  explicit Writer(port::Mutex* mu) : cv(mu) {}
};

Status SilkStore::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == nullptr);
  uint64_t last_sequence = max_sequence_;
  Writer* last_writer = &w;

  // Logless write
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    size_t nums = WriteBatchInternal::Count(updates);
    last_sequence += nums;
    {
      mutex_.Unlock();
      status = WriteBatchInternal::InsertInto(updates, mem_);
      mem_->AddCounter(nums);
      mutex_.Lock();
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    max_sequence_ = last_sequence;
  }
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }
  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
  return status;
}

bool SilkStore::GetProperty(const Slice& property, std::string* value) {
  if (property.ToString() == "silkstore.runs_searched") {
    *value = std::to_string(runs_searched) + "\n";
    value->append("runs_hit_counts: ");
    value->append(std::to_string(runs_hit_counts) + "\n");
    value->append("runs_miss_counts: ");
    value->append(std::to_string(runs_miss_counts) + "\n");
    value->append("bloom_filter_counts: ");
    value->append(std::to_string(bloom_filter_counts) + "\n");
    return true;
  } else if (property.ToString() == "silkstore.num_leaves") {
    auto it = leaf_index_->NewIterator(ReadOptions{});
    DeferCode c([it]() { delete it; });
    int cnt = 0;
    std::map<int, int> counts;
    it->SeekToFirst();
    while (it->Valid()) {
      ++cnt;
      LeafIndexEntry index_entry(it->value());
      int nums = index_entry.GetNumMiniRuns();
      counts[nums]++;
      it->Next();
    }
    std::cout << "NvmLeafIndex NumMiniRuns\n";
    for (auto it : counts) {
      std::cout << "NumMiniRuns: " << it.first << " count " << it.second
                << "\n";
    }
    *value = std::to_string(cnt);

    return true;
  } else if (property.ToString() == "silkstore.leaf_stats") {
    auto it = leaf_index_->NewIterator(ReadOptions{});
    DeferCode c([it]() { delete it; });
    int cnt = 0;
    it->SeekToFirst();
    while (it->Valid()) {
      ++cnt;
      auto key = it->key();
      LeafIndexEntry index_entry(it->value());
      value->append(key.ToString());
      value->append("->");
      value->append(index_entry.ToString());
      value->append(" ");
      it->Next();
    }
    return true;
  } else if (property.ToString() == "silkstore.leaf_avg_num_runs") {
    auto it = leaf_index_->NewIterator(ReadOptions{});
    DeferCode c([it]() { delete it; });
    int leaf_cnt = 0;
    int run_cnt = 0;
    it->SeekToFirst();
    while (it->Valid()) {
      ++leaf_cnt;
      auto key = it->key();
      LeafIndexEntry index_entry(it->value());
      run_cnt += index_entry.GetNumMiniRuns();
      it->Next();
    }
    *value = std::to_string(run_cnt / (leaf_cnt + 0.001));
    return true;
  } else if (property.ToString() == "silkstore.searches_in_memtable") {
    MutexLock g(&mutex_);
    size_t res = mem_->Searches();
    if (imm_) {
      res += imm_->Searches();
    }
    *value = std::to_string(res);
    return true;
  } else if (property.ToString() == "silkstore.gcstat") {
    *value =
        "\ntime spent in gc: " + std::to_string(stats_.time_spent_gc) + "us\n";
    return true;
  } else if (property.ToString() == "silkstore.segment_util") {
    *value = this->SegmentsSpaceUtilityHistogram();
    return true;
  } else if (property.ToString() == "silkstore.stats") {
    char buf[1000];
    snprintf(buf, sizeof(buf),
             "\nbytes rd %lu\n"
             "bytes wt %lu\n"
             "bytes rd gc %lu\n"
             "bytes rd gc %lu (Actual)\n"
             "bytes wt gc %lu\n"
             "# miniruns checked for gc %lu\n"
             "# miniruns queried for gc %lu\n",
             stats_.bytes_read, stats_.bytes_written,
             stats_.gc_bytes_read_unopt, stats_.gc_bytes_read,
             stats_.gc_bytes_written, stats_.gc_miniruns_total,
             stats_.gc_miniruns_queried);

    *value = buf;
    std::string leaf_index_stats;
    leaf_index_->GetProperty("leveldb.stats", &leaf_index_stats);
    value->append(leaf_index_stats);
    return true;
  } else if (property.ToString() == "silkstore.write_volume") {
    *value = std::to_string(stats_.bytes_written);
    return true;
  }
  return false;
}

Status SilkStore::Get(const ReadOptions& options, const Slice& key,
                      std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = max_sequence_;
  }
  NvmemTable* mem = mem_;
  NvmemTable* imm = imm_;
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = leaf_store_->Get(options, lkey, value, stat_store_);
    }
    mutex_.Lock();
  }

  //    if (have_stat_update && current->UpdateStats(stats)) {
  //        MaybeScheduleCompaction();
  //    }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  return s;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* SilkStore::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);
  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

class GroupedSegmentAppender {
 public:
  GroupedSegmentAppender(int num_groups, SegmentManager* segment_manager,
                         const Options& options,
                         bool gc_on_segment_shortage = true)
      : builders(num_groups),
        segment_manager(segment_manager),
        options(options),
        gc_on_segment_shortage(gc_on_segment_shortage) {}

  // Make sure the segment that is being built by a group has enough space.
  // If not, finish off the old segment and create a new segment.
  // Return the builder of the designated group.
  Status MakeRoomForGroupAndGetBuilder(uint32_t group_id,
                                       SegmentBuilder** builder_ptr,
                                       bool& switched_segment) {
    assert(group_id >= 0 && group_id < builders.size());
    if (builders[group_id] != nullptr &&
        builders[group_id]->FileSize() < options.segment_file_size_thresh) {
      // Segment of the group is in good shape, return its builder directly.
      *builder_ptr = builders[group_id];
      return Status::OK();
    } else if (builders[group_id] != nullptr &&
               builders[group_id]->FileSize() >=
                   options.segment_file_size_thresh) {
      // Segment filled up
      Status s = builders[group_id]->Finish();
      if (!s.ok()) {
        return s;
      }
      delete builders[group_id];
      builders[group_id] = nullptr;
    }
    uint32_t seg_id;
    std::unique_ptr<SegmentBuilder> new_builder;
    Status s = segment_manager->NewSegmentBuilder(&seg_id, new_builder,
                                                  gc_on_segment_shortage);
    if (!s.ok()) {
      return s;
    }
    switched_segment = true;
    *builder_ptr = builders[group_id] = new_builder.release();
    return Status::OK();
  }

  ~GroupedSegmentAppender() {
    // Finish off unfinished segments
    for (size_t i = 0; i < builders.size(); ++i) {
      if (builders[i] == nullptr) continue;
      builders[i]->Finish();
      delete builders[i];
      builders[i] = nullptr;
    }
  }

 private:
  std::vector<SegmentBuilder*> builders;
  SegmentManager* segment_manager;
  Options options;
  bool gc_on_segment_shortage;
};

std::pair<uint32_t, uint32_t> SilkStore::ChooseLeafCompactionRunRange(
    const LeafIndexEntry& leaf_index_entry) {
  // TODO: come up with a better approach
  uint32_t num_runs = leaf_index_entry.GetNumMiniRuns();
  assert(num_runs > 1);
  return {num_runs - 2, num_runs - 1};
}

LeafIndexEntry SilkStore::CompactLeaf(SegmentBuilder* seg_builder,
                                      uint32_t seg_no,
                                      const LeafIndexEntry& leaf_index_entry,
                                      Status& s, std::string* buf,
                                      uint32_t start_minirun_no,
                                      uint32_t end_minirun_no,
                                      const Snapshot* leaf_index_snap) {
  buf->clear();
  bool cover_whole_range = end_minirun_no - start_minirun_no + 1 ==
                           leaf_index_entry.GetNumMiniRuns();
  ReadOptions ropts;
  ropts.snapshot = leaf_index_snap;
  Iterator* it = leaf_store_->NewIteratorForLeaf(
      ropts, leaf_index_entry, s, start_minirun_no, end_minirun_no);
  if (!s.ok()) return {};
  DeferCode c([it]() { delete it; });

  it->SeekToFirst();
  std::string current_user_key;
  bool has_current_user_key = false;
  size_t num_unique_keys = 0, keys = 0;
  while (it->Valid()) {
    Slice key = it->key();
    ++keys;
    ParsedInternalKey ikey;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
    } else {
      auto itvalue = it->value();
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;

        if (cover_whole_range && ikey.type == kTypeDeletion) {
          // If all miniruns are compacted into one and the key type is
          // Deletion, then we can deleting this key physically by not adding it
          // to the final compacted run.
        } else {
          if (seg_builder->RunStarted() == false) {
            s = seg_builder->StartMiniRun();
            if (!s.ok()) return {};
          }
          ++num_unique_keys;
          seg_builder->Add(it->key(), itvalue);
        }
      }
    }
    it->Next();
  }

  LeafIndexEntry new_leaf_index_entry;

  if (seg_builder->RunStarted() == false) {
    // The result of the compacted range is empty, remove them from the index
    // entry
    s = LeafIndexEntryBuilder::RemoveMiniRunRange(
        leaf_index_entry, start_minirun_no, end_minirun_no, buf,
        &new_leaf_index_entry);
  } else {
    uint32_t run_no;
    seg_builder->FinishMiniRun(&run_no);
    // Otherwise, replace the compacted range minirun index entries with the
    // result minirun index entry
    std::string buf2;
    MiniRunIndexEntry replacement = MiniRunIndexEntry::Build(
        seg_no, run_no, seg_builder->GetFinishedRunIndexBlock(),
        seg_builder->GetFinishedRunFilterBlock(),
        seg_builder->GetFinishedRunDataSize(), &buf2);
    s = LeafIndexEntryBuilder::ReplaceMiniRunRange(
        leaf_index_entry, start_minirun_no, end_minirun_no, replacement, buf,
        &new_leaf_index_entry);
  }

  if (!s.ok()) return {};
  return new_leaf_index_entry;
}

Status SilkStore::CopyMinirunRun(Slice leaf_max_key,
                                 LeafIndexEntry& leaf_index_entry,
                                 uint32_t run_idx_in_index_entry,
                                 SegmentBuilder* target_seg_builder,
                                 WriteBatch& leaf_index_wb) {
  Status s;
  assert(run_idx_in_index_entry < leaf_index_entry.GetNumMiniRuns());
  std::unique_ptr<Iterator> source_it(leaf_store_->NewIteratorForLeaf(
      {}, leaf_index_entry, s, run_idx_in_index_entry, run_idx_in_index_entry));
  if (!s.ok()) return s;
  assert(target_seg_builder->RunStarted() == false);
  source_it->SeekToFirst();
  s = target_seg_builder->StartMiniRun();
  if (!s.ok()) return s;
  while (source_it->Valid()) {
    target_seg_builder->Add(source_it->key(), source_it->value());
    source_it->Next();
  }
  uint32_t run_no;
  s = target_seg_builder->FinishMiniRun(&run_no);
  if (!s.ok()) return s;
  std::string buf;
  buf.clear();
  MiniRunIndexEntry new_minirun_index_entry = MiniRunIndexEntry::Build(
      target_seg_builder->SegmentId(), run_no,
      target_seg_builder->GetFinishedRunIndexBlock(),
      target_seg_builder->GetFinishedRunFilterBlock(),
      target_seg_builder->GetFinishedRunDataSize(), &buf);
  LeafIndexEntry new_leaf_index_entry;
  std::string buf2;
  s = LeafIndexEntryBuilder::ReplaceMiniRunRange(
      leaf_index_entry, run_idx_in_index_entry, run_idx_in_index_entry,
      new_minirun_index_entry, &buf2, &new_leaf_index_entry);
  if (!s.ok()) return s;
  leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
  return s;
}

Status SilkStore::GarbageCollectSegment(Segment* seg,
                                        GroupedSegmentAppender& appender,
                                        WriteBatch& leaf_index_wb) {
  Status s;
  size_t copied = 0;
  size_t segment_size = seg->SegmentSize();

  seg->ForEachRun([&, this](int run_no, MiniRunHandle run_handle,
                            size_t run_size, bool valid) {
    stats_.AddGCUnoptStats(
        std::max(options_.block_size, run_handle.last_block_handle.size()));
    if (valid == false) {  // Skip invalidated runs
      stats_.AddGCMiniRunStats(0, 1);
      return false;
    }
    stats_.AddGCMiniRunStats(1, 1);
    // We take out the first key of the last block in the run to query
    // leaf_index for validness
    MiniRun* run;
    Block index_block(BlockContents({Slice(), false, false}));
    s = seg->OpenMiniRun(run_no, index_block, &run);
    if (!s.ok())  // error, early exit
      return true;
    DeferCode c([run]() { delete run; });
    BlockHandle last_block_handle = run_handle.last_block_handle;

    std::unique_ptr<Iterator> block_it(
        run->NewIteratorForOneBlock({}, last_block_handle));

    // Read the last block aligned by options_.block_size
    stats_.AddGCStats(std::max(options_.block_size, last_block_handle.size()),
                      0);
    stats_.Add(std::max(options_.block_size, last_block_handle.size()), 0);
    block_it->SeekToFirst();
    if (block_it->Valid()) {
      auto internal_key = block_it->key();
      ParsedInternalKey parsed_internal_key;
      if (!ParseInternalKey(internal_key, &parsed_internal_key)) {
        s = Status::InvalidArgument(
            "invalid key found during segment scan for GC");
        return true;
      }
      auto user_key = parsed_internal_key.user_key;
      std::unique_ptr<Iterator> leaf_it(leaf_index_->NewIterator({}));
      leaf_it->Seek(user_key);
      if (!leaf_it->Valid()) return false;

      Slice leaf_key = leaf_it->key();
      LeafIndexEntry leaf_index_entry = leaf_it->value();

      uint32_t run_idx_in_index_entry = leaf_index_entry.GetNumMiniRuns();
      uint32_t seg_id = seg->SegmentId();
      leaf_index_entry.ForEachMiniRunIndexEntry(
          [&run_idx_in_index_entry, run_no, seg_id](
              const MiniRunIndexEntry& minirun_index_entry, uint32_t idx) {
            if (minirun_index_entry.GetSegmentNumber() == seg_id &&
                minirun_index_entry.GetRunNumberWithinSegment() ==
                    run_no) {  // Found that the index entry stored in
                               // leaf_index_ is still pointing to this run in
                               // this segment
              run_idx_in_index_entry = idx;
              return true;
            }
            return false;
          },
          LeafIndexEntry::TraversalOrder::forward);

      if (run_idx_in_index_entry ==
          leaf_index_entry.GetNumMiniRuns())  // Stale minirun, skip it
        return false;

      SegmentBuilder* seg_builder;
      bool switched_segment = false;
      s = appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder,
                                                 switched_segment);
      if (!s.ok())  // error, early exit
        return true;
      // Copy the entire minirun to the other segment file and update leaf_index
      // accordingly
      s = CopyMinirunRun(leaf_key, leaf_index_entry, run_idx_in_index_entry,
                         seg_builder, leaf_index_wb);
      if (!s.ok())  // error, early exit
        return true;
      // Read from the old leaf
      // Write to the new leaf
      stats_.Add(leaf_index_entry.GetLeafDataSize(),
                 leaf_index_entry.GetLeafDataSize());
      stats_.AddGCStats(leaf_index_entry.GetLeafDataSize(),
                        leaf_index_entry.GetLeafDataSize());
      copied += run_size;
    }
    return false;
  });
  // if (copied)
  // fprintf(stderr, "Copied %f%% the data from segment %d of size %lu\n",
  // (copied+0.0)/segment_size * 100, seg->SegmentId(), segment_size);
  return Status::OK();
}

std::string SilkStore::SegmentsSpaceUtilityHistogram() {
  MutexLock g(&GCMutex);
  Histogram hist;
  Status s;
  hist.Clear();
  size_t total_segment_size = 0;
  size_t total_valid_size = 0;
  segment_manager_->ForEachSegment([&, this](Segment* seg) {
    size_t seg_size = seg->SegmentSize();
    total_segment_size += seg_size;
    size_t valid_size = 0;
    bool error = false;
    seg->ForEachRun([&, this](int run_no, MiniRunHandle run_handle,
                              size_t run_size, bool valid) {
      if (valid == false) {  // Skip invalidated runs
        return false;
      }
      // Maybe valid, we'll see.

      // We take out the first key of the last block in the run to query
      // leaf_index for validness
      MiniRun* run;
      Block index_block(BlockContents({Slice(), false, false}));
      s = seg->OpenMiniRun(run_no, index_block, &run);
      if (!s.ok()) {
        // error, early exit
        error = true;
        return true;
      }

      DeferCode c([run]() { delete run; });
      BlockHandle last_block_handle = run_handle.last_block_handle;

      std::unique_ptr<Iterator> block_it(
          run->NewIteratorForOneBlock({}, last_block_handle));

      // Read the last block aligned by options_.block_size
      block_it->SeekToFirst();
      if (block_it->Valid()) {
        auto internal_key = block_it->key();
        ParsedInternalKey parsed_internal_key;
        if (!ParseInternalKey(internal_key, &parsed_internal_key)) {
          s = Status::InvalidArgument(
              "invalid key found during segment scan for GC");
          error = true;
          return true;
        }
        auto user_key = parsed_internal_key.user_key;
        std::unique_ptr<Iterator> leaf_it(leaf_index_->NewIterator({}));
        leaf_it->Seek(user_key);
        if (!leaf_it->Valid()) return false;

        LeafIndexEntry leaf_index_entry = leaf_it->value();

        uint32_t run_idx_in_index_entry = leaf_index_entry.GetNumMiniRuns();
        uint32_t seg_id = seg->SegmentId();
        leaf_index_entry.ForEachMiniRunIndexEntry(
            [&run_idx_in_index_entry, run_no, seg_id](
                const MiniRunIndexEntry& minirun_index_entry, uint32_t idx) {
              if (minirun_index_entry.GetSegmentNumber() == seg_id &&
                  minirun_index_entry.GetRunNumberWithinSegment() ==
                      run_no) {  // Found that the index entry stored in
                                 // leaf_index_ is still pointing to this run in
                                 // this segment
                run_idx_in_index_entry = idx;
                return true;
              }
              return false;
            },
            LeafIndexEntry::TraversalOrder::forward);

        if (run_idx_in_index_entry ==
            leaf_index_entry.GetNumMiniRuns())  // Stale minirun, skip it
          return false;

        valid_size += run_size;
      }
      return false;
    });
    if (error == false) {
      assert(valid_size <= seg_size);
      double util = (valid_size + 0.0) / seg_size;
      hist.Add(util * 100);
      total_valid_size += valid_size;
    }
  });
  return hist.ToString() +
         "\ntotal_valid_size: " + std::to_string(total_valid_size) +
         "\ntotal_segment_size : " + std::to_string(total_segment_size) + "\n";
}

int SilkStore::GarbageCollect() {
  MutexLock g(&GCMutex);
  Log(options_.info_log, "Garbage Collect(gc).");
  WriteBatch leaf_index_wb;
  // Simple policy: choose the segment with maximum number of invalidated runs
  constexpr int kGCSegmentCandidateNum = 5;
  std::vector<Segment*> candidates =
      segment_manager_->GetMostInvalidatedSegments(kGCSegmentCandidateNum);
  if (candidates.empty()) return 0;
  // Disable nested garbage collection
  bool gc_on_segment_shortage = false;
  GroupedSegmentAppender appender(1, segment_manager_, options_,
                                  gc_on_segment_shortage);
  for (auto seg : candidates) {
    GarbageCollectSegment(seg, appender, leaf_index_wb);
  }

  if (leaf_index_wb.ApproximateSize()) {
    leaf_index_->Write({}, &leaf_index_wb);
  }
  for (auto seg : candidates) {
    segment_manager_->RemoveSegment(seg->SegmentId());
  }
  printf("gc collect %lu\n", candidates.size());
  Log(options_.info_log, "gc collect %lu\n", candidates.size());

  return candidates.size();
}

Status SilkStore::InvalidateLeafRuns(const LeafIndexEntry& leaf_index_entry,
                                     size_t start_minirun_no,
                                     size_t end_minirun_no) {
  Status s = Status::OK();
  leaf_index_entry.ForEachMiniRunIndexEntry(
      [&](const MiniRunIndexEntry& index_entry, uint32_t no) -> bool {
        if (start_minirun_no <= no && no <= end_minirun_no) {
          s = segment_manager_->InvalidateSegmentRun(
              index_entry.GetSegmentNumber(),
              index_entry.GetRunNumberWithinSegment());
          if (!s.ok()) {
            return true;
          }
        }
        return false;
      },
      LeafIndexEntry::TraversalOrder::forward);
  return s;
}

Status SilkStore::OptimizeLeaf() {
  Log(options_.info_log, "Updating read hotness for all leaves.");
  stat_store_.UpdateReadHotness();

  if (options_.enable_leaf_read_opt == false) return Status::OK();
  Log(options_.info_log,
      "Scanning for leaves that are suitable for optimization.");

  constexpr int kOptimizationK = 100;
  struct HeapItem {
    double read_hotness;
    std::shared_ptr<std::string> leaf_max_key;

    bool operator<(const HeapItem& rhs) const {
      return read_hotness < rhs.read_hotness;
    }
  };
  MutexLock g(&GCMutex);
  auto leaf_index_snapshot = leaf_index_->GetSnapshot();
  DeferCode c([this, &leaf_index_snapshot]() {
    leaf_index_->ReleaseSnapshot(leaf_index_snapshot);
  });

  // Maintain a min-heap of kOptimizationK elements based on read-hotness
  std::priority_queue<HeapItem> candidate_heap;

  stat_store_.ForEachLeaf(
      [this, &candidate_heap](const std::string& leaf_max_key,
                              const LeafStatStore::LeafStat& stat) {
        double read_hotness = stat.read_hotness;
        if (stat.num_runs >= 2 /* options_.leaf_max_num_miniruns / 4 */ &&
            read_hotness > 0) {
          if (candidate_heap.size() < kOptimizationK) {
            candidate_heap.push(HeapItem{
                read_hotness, std::make_shared<std::string>(leaf_max_key)});
          } else {
            if (read_hotness > candidate_heap.top().read_hotness) {
              candidate_heap.pop();
              candidate_heap.push(HeapItem{
                  read_hotness, std::make_shared<std::string>(leaf_max_key)});
            }
          }
        }
      });

  std::unique_ptr<SegmentBuilder> seg_builder;
  uint32_t seg_id;
  Status s;
  std::string buf;

  if (candidate_heap.size()) {
    bool gc_on_segment_shortage = true;
    s = segment_manager_->NewSegmentBuilder(&seg_id, seg_builder,
                                            gc_on_segment_shortage);
    if (!s.ok()) {
      return s;
    }
  } else {
    return s;
  }
  WriteBatch leaf_index_wb;
  int compacted_runs = 0;
  // Now candidate_heap contains kOptimizationK leaves with largest read-hotness
  // and ready for optimization
  while (!candidate_heap.empty()) {
    if (seg_builder->FileSize() > options_.segment_file_size_thresh) {
      s = seg_builder->Finish();
      // fprintf(stderr, "Segment %d filled up, creating a new one\n", seg_id);
      if (!s.ok()) {
        return s;
      }
      bool gc_on_segment_shortage = true;
      s = segment_manager_->NewSegmentBuilder(&seg_id, seg_builder,
                                              gc_on_segment_shortage);
      if (!s.ok()) {
        return s;
      }
      s = leaf_index_->Write(WriteOptions{}, &leaf_index_wb);
      if (!s.ok()) {
        return s;
      }
      leaf_index_wb.Clear();
    }
    HeapItem item = candidate_heap.top();
    candidate_heap.pop();
    ReadOptions ropts;
    ropts.snapshot = leaf_index_snapshot;
    std::string leaf_index_entry_payload;
    s = leaf_index_->Get(ropts, *item.leaf_max_key, &leaf_index_entry_payload);
    if (!s.ok()) continue;
    LeafIndexEntry index_entry(leaf_index_entry_payload);
    // fprintf(stderr, "optimization candidate leaf key %s, Rh %lf, compacting \
    // miniruns[%d, %d]\n", item.leaf_max_key->c_str(), item.read_hotness, 0,
    // index_entry.GetNumMiniRuns() - 1);
    assert(seg_builder->RunStarted() == false);
    LeafIndexEntry new_index_entry =
        CompactLeaf(seg_builder.get(), seg_id, index_entry, s, &buf, 0,
                    index_entry.GetNumMiniRuns() - 1, leaf_index_snapshot);
    assert(seg_builder->RunStarted() == false);
    if (!s.ok()) {
      return s;
    }
    leaf_index_wb.Put(Slice(*item.leaf_max_key), new_index_entry.GetRawData());
    s = InvalidateLeafRuns(index_entry, 0, index_entry.GetNumMiniRuns() - 1);
    if (!s.ok()) {
      return s;
    }
    compacted_runs += index_entry.GetNumMiniRuns();
    stat_store_.UpdateLeafNumRuns(*item.leaf_max_key, 1);
  }
  if (compacted_runs) {
    Log(options_.info_log, "Leaf Optimization compacted %d runs\n",
        compacted_runs);
    // fprintf(stderr, "Leaf Optimization compacted %d runs\n", compacted_runs);
  }
  if (seg_builder.get()) {
    return seg_builder->Finish();
  }
  if (leaf_index_wb.ApproximateSize()) {
    return leaf_index_->Write(WriteOptions{}, &leaf_index_wb);
  }
  return s;
}

constexpr size_t kLeafIndexWriteBufferMaxSize = 4 * 1024 * 1024;

void SilkStore::PrepareLeafsNeedSplit(bool force) {
  ReadOptions ro;
  ro.snapshot = leaf_index_->GetSnapshot();
  // Release snapshot after the traversal is done
  DeferCode c([&ro, this]() { leaf_index_->ReleaseSnapshot(ro.snapshot); });
  std::unique_ptr<Iterator> iit(leaf_index_->NewIterator(ro));
  iit->SeekToFirst();
  while (iit->Valid()) {
    LeafIndexEntry leaf_index_entry(iit->value());
    int num_miniruns = leaf_index_entry.GetNumMiniRuns();
    if (force || num_miniruns >= options_.leaf_max_num_miniruns) {
      // Log(options_.info_log, "MakeRoomInLeafLayer Process %d\n",
      // num_miniruns);
      leafs_need_split.emplace_back(iit->key().ToString(),
                                    iit->value().ToString());
    }
    // Record the data read from leaf_index as well
    stats_.Add(iit->key().size() + iit->value().size(), 0);
    iit->Next();
  }
  split_subtask_states_.resize(split_leaf_num_threads_);
  //    for (auto & kv:leafs_need_split) {
  //        std::string k = kv.max_key_, v = kv.value_;
  //        Log(options_.info_log, "k: %s  v: %s\n", k.c_str(), v.c_str());
  //    }
  Log(options_.info_log, "total kv size: %ld\n", leafs_need_split.size());
}

void SilkStore::ProcessOneLeaf(
    SingleLeaf& leaf, SplitLeafTaskState& state,
    GroupedSegmentAppender& grouped_segment_appender) {
  WriteBatch& leaf_index_wb = state.leaf_index_wb_;
  SequenceNumber seq_num = max_sequence_;

  auto SplitLeaf = [&grouped_segment_appender, &leaf_index_wb, this](
                       const LeafIndexEntry& leaf_index_entry,
                       SequenceNumber seq_num,
                       std::vector<std::string>& max_keys,
                       std::vector<std::string>& max_key_index_entry_bufs) {
    Status s;
    /* We use DBIter to get the most recent non-deleted keys. */
    auto it = dynamic_cast<silkstore::DBIter*>(leaf_store_->NewDBIterForLeaf(
        ReadOptions{}, leaf_index_entry, s, user_comparator(), seq_num));

    DeferCode c([it]() { delete it; });

    it->SeekToFirst();

    size_t bytes_current_leaf = 0;

    SegmentBuilder* seg_builder = nullptr;
    auto AssignSegmentBuilder = [&seg_builder, &grouped_segment_appender,
                                 &leaf_index_wb, this]() {
      bool switched_segment = false;
      Status s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(
          0, &seg_builder, switched_segment);
      if (!s.ok()) return s;

      if (switched_segment &&
          leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
        // If all previous segments are built successfully and
        // the leaf_index write buffer exceeds the threshold,
        // write it down to leaf_index_ to keep the memory footprint small.
        s = leaf_index_->Write({}, &leaf_index_wb);
        if (!s.ok()) return s;
        leaf_index_wb.Clear();
      }
      return Status::OK();
    };

    std::string max_key;
    std::string max_key_index_entry_buf;

    while (it->Valid()) {
      if (seg_builder == nullptr) {
        s = AssignSegmentBuilder();
        if (!s.ok()) return s;
        seg_builder->StartMiniRun();
      }
      bytes_current_leaf += it->internal_key().size() + it->value().size();

      // Since splitting a leaf should preserve the sequence numbers of the most
      // recent non-deleted keys,
      // we modified DBIter to provide access to its internal key
      // representation.
      seg_builder->Add(it->internal_key(), it->value());
      max_key = it->key().ToString();
      it->Next();
      if (bytes_current_leaf >= options_.leaf_datasize_thresh / 2 ||
          it->Valid() == false) {
        uint32_t run_no;
        seg_builder->FinishMiniRun(&run_no);
        max_key_index_entry_buf.clear();
        std::string buf;
        MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(
            seg_builder->SegmentId(), run_no,
            seg_builder->GetFinishedRunIndexBlock(),
            seg_builder->GetFinishedRunFilterBlock(),
            seg_builder->GetFinishedRunDataSize(), &buf);
        LeafIndexEntry new_leaf_index_entry;
        LeafIndexEntryBuilder::AppendMiniRunIndexEntry(
            LeafIndexEntry{}, minirun_index_entry, &max_key_index_entry_buf,
            &new_leaf_index_entry);
        max_keys.push_back(max_key);
        max_key_index_entry_bufs.push_back(max_key_index_entry_buf);
        // Log(options_.info_log, "%d Split k: %s   v: %s\n", max_keys.size(),
        // max_key.c_str(), max_key_index_entry_buf.c_str());
        if (it->Valid() == true) {
          // If there are more key values left, keep working.
          s = AssignSegmentBuilder();
          if (!s.ok()) return s;
          seg_builder->StartMiniRun();
        }
        bytes_current_leaf = 0;
      }
    }

    return Status::OK();
  };

  Status& s = state.s_;
  Slice leaf_max_key(leaf.max_key_);
  LeafIndexEntry leaf_index_entry(leaf.value_);

  SegmentBuilder* seg_builder = nullptr;
  bool switched_segment = false;
  s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder,
                                                             switched_segment);
  if (!s.ok()) return;

  if (switched_segment &&
      leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
    // If all previous segments are built successfully and
    // the leaf_index write buffer exceeds the threshold,
    // write it down to leaf_index_ to keep the memory footprint small.
    s = leaf_index_->Write({}, &leaf_index_wb);
    if (!s.ok()) return;
    leaf_index_wb.Clear();
  }

  // uint32_t seg_id = seg_builder->SegmentId();

  // if ((leaf_index_entry.GetLeafDataSize() >= options_.leaf_datasize_thresh))
  // { fprintf(stderr, "Splitting leaf with max key %s at sequence num %lu
  // segment %d\n",
  //        leaf_max_key.ToString().c_str(), seq_num, seg_id);
  std::vector<std::string> max_keys;
  std::vector<std::string> max_key_index_entry_bufs;
  // Log(options_.info_log, "Start split k: %s\n", leaf.max_key_.c_str());
  s = SplitLeaf(leaf_index_entry, seq_num, max_keys, max_key_index_entry_bufs);
  assert(max_keys.size() == max_key_index_entry_bufs.size());
  if (!s.ok()) return;
  //++num_splits;
  // Invalidate the miniruns pointed by the old leaf index entry
  s = InvalidateLeafRuns(leaf_index_entry, 0,
                         leaf_index_entry.GetNumMiniRuns() - 1);
  if (!s.ok()) {
    return;
  }

  // Update the index entries
  stat_store_.SplitLeaf(leaf_max_key.ToString(), max_keys);
  leaf_index_wb.Delete(leaf_max_key);
  //--num_leaves;
  --(state.leaf_change_num_);
  for (size_t i = 0; i < max_keys.size(); ++i) {
    leaf_index_wb.Put(Slice(max_keys[i]), Slice(max_key_index_entry_bufs[i]));
    stat_store_.UpdateLeafNumRuns(max_keys[i], 1);
  }
  // num_leaves += max_keys.size();
  state.leaf_change_num_ += max_keys.size();
  // Read all data and merge them, then write all out
  // stats_.Add(leaf_index_entry.GetLeafDataSize(),
  // leaf_index_entry.GetLeafDataSize());
  state.read_ += leaf_index_entry.GetLeafDataSize();
  state.written_ += leaf_index_entry.GetLeafDataSize();
}

void SilkStore::ProcessSplitLeafSubTasks(int tid) {
  GroupedSegmentAppender grouped_segment_appender(1, segment_manager_,
                                                  options_);

  for (size_t i = 0; i < leafs_need_split.size(); ++i) {
    if (tid == (i % split_leaf_num_threads_)) {
      ProcessOneLeaf(leafs_need_split[i], split_subtask_states_[tid],
                     grouped_segment_appender);
      // failed compaction
      if (!split_subtask_states_[tid].s_.ok()) {
        break;
      }
    }
  }
}

void SilkStore::RunSplitLeafTasks() {
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(split_leaf_num_threads_ - 1);

  for (size_t i = 1; i < split_leaf_num_threads_; ++i) {
    thread_pool.emplace_back(&SilkStore::ProcessSplitLeafSubTasks, this, i);
  }

  // Always schedule the first task(whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessSplitLeafSubTasks(0);

  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }
}

Status SilkStore::FinishSplitLeafTasks() {
  DeferCode c([this]() {
    leafs_need_split.clear();
    split_subtask_states_.clear();
  });

  for (auto& state : split_subtask_states_) {
    if (!state.s_.ok()) {
      Log(options_.info_log, "MakeRoomInLeafLayer failed: %s\n",
          state.s_.ToString().c_str());
      return state.s_;
    }

    // Record the read and write
    stats_.Add(state.read_, state.written_);
    // Record the change of leaf num
    num_leaves += state.leaf_change_num_;

    if (state.leaf_index_wb_.ApproximateSize()) {
      Status s = leaf_index_->Write({}, &(state.leaf_index_wb_));
      if (!s.ok()) {
        Log(options_.info_log, "leaf_index_->Write failed: %s\n",
            s.ToString().c_str());
        return s;
      }
      state.leaf_index_wb_.Clear();
    }
  }
  return Status();
}

Status SilkStore::MakeRoomInLeafLayer(bool force) {
  Log(options_.info_log, "MakeRoomInLeafLayer Start\n");
  mutex_.Unlock();

restart : {
  DeferCode c([this]() { mutex_.Lock(); });
  PrepareLeafsNeedSplit(force);
  RunSplitLeafTasks();
  Status s = FinishSplitLeafTasks();
  Log(options_.info_log, "MakeRoomInLeafLayer End\n");
  return s;
};
}

static int num_compactions = 0;

void SilkStore::GenSubcompactionBoundaries() {
  ReadOptions ro;
  ro.snapshot = leaf_index_->GetSnapshot();
  // Release snapshot after the traversal is done
  DeferCode c([&ro, this]() { leaf_index_->ReleaseSnapshot(ro.snapshot); });

  std::unique_ptr<Iterator> iit(leaf_index_->NewIterator(ro));
  iit->SeekToFirst();

  std::string key_buf, value_buf;

  while (iit->Valid()) {
    // itt return the value for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    key_buf.clear();
    key_buf.append(iit->key().data(), iit->key().size());
    boundries_.emplace_back(key_buf);

    value_buf.clear();
    value_buf.append(iit->value().data(), iit->value().size());
    leaf_values_.push_back(value_buf);
    // Record the data read from leaf_index as well
    stats_.Add(iit->key().size() + iit->value().size(), 0);
    iit->Next();
  }
}

void SilkStore::PrepareCompactionTasks() {
  GenSubcompactionBoundaries();
  size_t size = boundries_.size();
  sub_compact_tasks_.reserve(size + 1);
  for (size_t i = 0; i <= size; ++i) {
    // nullptr means unbounded
    std::string* start = (i == 0) ? nullptr : &boundries_[i - 1];
    std::string* end = (i == size) ? nullptr : &boundries_[i];
    // nullptr means the leaf is not exist
    std::string* value = (i == size) ? nullptr : &leaf_values_[i];
    sub_compact_tasks_.emplace_back(start, end, value);
  }
  compact_subtask_states_.resize(compact_num_threads_);
}

void SilkStore::ProcessKeyValueCompaction(
    SubCompaction& sub_compact, CompactSubTaskState& state,
    GroupedSegmentAppender& grouped_segment_appender) {
  std::string* start = sub_compact.start_;
  std::string* end = sub_compact.end_;
  std::string* leaf_value = sub_compact.value_;
  Status& s = state.s_;

  WriteBatch& leaf_index_wb = state.leaf_index_wb_;

  std::unique_ptr<Iterator> mit(imm_->NewIterator());

  SegmentBuilder* seg_builder = nullptr;
  bool switched_segment = false;
  s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(0, &seg_builder,
                                                             switched_segment);
  if (!s.ok()) return;

  if (switched_segment &&
      leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
    // If all previous segments are built successfully and
    // the leaf_index write buffer exceeds the threshold,
    // write it down to leaf_index_ to keep the memory footprint small.
    s = leaf_index_->Write({}, &leaf_index_wb);
    if (!s.ok()) return;
    leaf_index_wb.Clear();
  }

  uint32_t seg_id = seg_builder->SegmentId();

  assert(seg_builder->RunStarted() == false);

  if (start == nullptr) {
    mit->SeekToFirst();
  } else {
    Slice start_slice(*start);

    SequenceNumber snapshot = max_sequence_;
    LookupKey lkey(start_slice, snapshot);

    // seek to the first key larger than *start
    mit->Seek(lkey.memtable_key());
    // mit->SeekToFirst();
    while (mit->Valid() && s.ok()) {
      Slice imm_internal_key = mit->key();
      ParsedInternalKey parsed_internal_key;
      if (!ParseInternalKey(imm_internal_key, &parsed_internal_key)) {
        s = Status::InvalidArgument(
            "error parsing key from immutable table during compaction");
        return;
      }
      if (this->user_comparator()->Compare(parsed_internal_key.user_key,
                                           start_slice) > 0) {
        break;
      }
      mit->Next();
    }
  }

  if (end != nullptr) {
    Slice leaf_max_key(*end);
    Slice leaf_value_slice(*leaf_value);
    LeafIndexEntry leaf_index_entry(leaf_value_slice);

    // normal process
    int minirun_key_cnt = 0;
    // Build up a minirun of key value payloads
    while (mit->Valid() && s.ok()) {
      Slice imm_internal_key = mit->key();
      ParsedInternalKey parsed_internal_key;
      if (!ParseInternalKey(imm_internal_key, &parsed_internal_key)) {
        s = Status::InvalidArgument(
            "error parsing key from immutable table during compaction");
        return;
      }
      if (this->user_comparator()->Compare(parsed_internal_key.user_key,
                                           leaf_max_key) > 0) {
        break;
      }
      if (seg_builder->RunStarted() == false) {
        s = seg_builder->StartMiniRun();
        if (!s.ok()) {
          return;
        }
        assert(seg_builder->RunStarted());
      }
      seg_builder->Add(mit->key(), mit->value());

      // Reading data from memtable costs no read io.
      // Record the write to segment.
      state.written_ += (mit->key().size() + mit->value().size());
      ++minirun_key_cnt;

      mit->Next();
    }

    stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);

    std::string buf, buf2;
    uint32_t run_no;

    if (seg_builder->RunStarted()) {
      s = seg_builder->FinishMiniRun(&run_no);
      if (!s.ok()) {
        return;
      }
      // Generate an index entry for the new minirun
      buf.clear();
      MiniRunIndexEntry new_minirun_index_entry = MiniRunIndexEntry::Build(
          seg_id, run_no, seg_builder->GetFinishedRunIndexBlock(),
          seg_builder->GetFinishedRunFilterBlock(),
          seg_builder->GetFinishedRunDataSize(), &buf);
      // Update the leaf index entry
      LeafIndexEntry new_leaf_index_entry;
      LeafIndexEntryBuilder::AppendMiniRunIndexEntry(
          leaf_index_entry, new_minirun_index_entry, &buf2,
          &new_leaf_index_entry);

      assert(leaf_index_entry.GetNumMiniRuns() + 1 ==
             new_leaf_index_entry.GetNumMiniRuns());
      // Write out the updated entry to leaf index
      leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
      stat_store_.UpdateLeafNumRuns(leaf_max_key.ToString(),
                                    new_leaf_index_entry.GetNumMiniRuns());
    } else {
      // Memtable has no keys intersected with this leaf
      if (leaf_index_entry.Empty()) {
        // If the leaf became empty due to self-compaction or split, remove it
        // from the leaf index
        leaf_index_wb.Delete(leaf_max_key);
        --(state.leaf_change_num_);
        stat_store_.DeleteLeaf(leaf_max_key.ToString());
        // fprintf(stderr, "Deleted index entry for empty leaf of key %s\n",
        // leaf_max_key.ToString().c_str());
      }
    }

  } else {
    // create new leaf and process
    // Memtable has keys that are greater than all the keys in leaf_index_.
    // In this case, partition the rest of memtable contents into leaves each no
    // more than options_.leaf_datasize_thresh bytes in size.
    while (s.ok() && mit->Valid()) {
      std::string buf, buf2;

      SegmentBuilder* seg_builder = nullptr;
      bool switched_segment = false;
      s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(
          0, &seg_builder, switched_segment);
      if (!s.ok()) {
        return;
      }

      if (switched_segment &&
          leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
        // If all previous segments are built successfully and
        // the leaf_index write buffer exceeds the threshold,
        // write it down to leaf_index_ to keep the memory footprint small.
        s = leaf_index_->Write({}, &leaf_index_wb);
        if (!s.ok()) return;
        leaf_index_wb.Clear();
      }

      uint32_t seg_id = seg_builder->SegmentId();

      assert(seg_builder->RunStarted() == false);

      s = seg_builder->StartMiniRun();
      if (!s.ok()) {
        fprintf(stderr, "%s", s.ToString().c_str());
        return;
      }

      size_t bytes = 0;
      int minirun_key_cnt = 0;

      Slice leaf_max_key;

      while (mit->Valid()) {
        Slice imm_internal_key = mit->key();
        ParsedInternalKey parsed_internal_key;
        if (!ParseInternalKey(mit->key(), &parsed_internal_key)) {
          s = Status::InvalidArgument(
              "error parsing key from immutable table during compaction");
          fprintf(stderr, "%s", s.ToString().c_str());
          return;
        }

        // A leaf holds at least one key-value pair and at most
        // options_.leaf_datasize_thresh bytes of data.
        if (minirun_key_cnt > 0 &&
            bytes + imm_internal_key.size() + mit->value().size() >=
                options_.leaf_datasize_thresh * 0.05) {
          break;
        }

        bytes += imm_internal_key.size() + mit->value().size();

        leaf_max_key = parsed_internal_key.user_key;
        seg_builder->Add(imm_internal_key, mit->value());
        // Reading data from memtable costs no read io.
        // Record the write to segment.
        state.written_ += (mit->key().size() + mit->value().size());
        ++minirun_key_cnt;
        mit->Next();
      }

      uint32_t run_no;
      seg_builder->FinishMiniRun(&run_no);
      assert(seg_builder->GetFinishedRunDataSize());
      // Generate an index entry for the new minirun
      MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(
          seg_id, run_no, seg_builder->GetFinishedRunIndexBlock(),
          seg_builder->GetFinishedRunFilterBlock(),
          seg_builder->GetFinishedRunDataSize(), &buf);
      LeafIndexEntry new_leaf_index_entry;
      LeafIndexEntryBuilder::AppendMiniRunIndexEntry(
          LeafIndexEntry{}, minirun_index_entry, &buf2, &new_leaf_index_entry);
      leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
      ++(state.leaf_change_num_);
      stat_store_.NewLeaf(leaf_max_key.ToString());
      stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);
    }
  }
}

void SilkStore::ProcessCompactionSubTasks(int tid) {
  GroupedSegmentAppender grouped_segment_appender(1, segment_manager_,
                                                  options_);

  for (size_t i = 0; i < sub_compact_tasks_.size(); ++i) {
    if (tid == (i % compact_num_threads_)) {
      ProcessKeyValueCompaction(sub_compact_tasks_[i],
                                compact_subtask_states_[tid],
                                grouped_segment_appender);
      // failed compaction
      if (!compact_subtask_states_[tid].s_.ok()) {
        break;
      }
    }
  }
}

void SilkStore::RunCompactionTasks() {
  // Launch a thread for each of subcompactions 1...num_threads-1
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(compact_num_threads_ - 1);

  for (size_t i = 1; i < compact_num_threads_; ++i) {
    thread_pool.emplace_back(&SilkStore::ProcessCompactionSubTasks, this, i);
  }

  // Always schedule the first subcompaction (whether or not there are also
  // others) in the current thread to be efficient with resources
  ProcessCompactionSubTasks(0);

  // Wait for all other threads (if there are any) to finish execution
  for (auto& thread : thread_pool) {
    thread.join();
  }
}

Status SilkStore::FinishCompactionTasks() {
  DeferCode c([this]() {
    sub_compact_tasks_.clear();
    compact_subtask_states_.clear();
    boundries_.clear();
    leaf_values_.clear();
  });

  for (auto& state : compact_subtask_states_) {
    if (!state.s_.ok()) {
      return state.s_;
    }
    // Record the read and write
    stats_.Add(state.read_, state.written_);
    // Record the change of leaf num
    num_leaves += state.leaf_change_num_;

    if (state.leaf_index_wb_.ApproximateSize()) {
      Status s = leaf_index_->Write({}, &(state.leaf_index_wb_));
      if (!s.ok()) return s;
      state.leaf_index_wb_.Clear();
    }
  }
  return Status();
}

/*
// Discard: we should not use this version
Status SilkStore::DoCompactionWork(WriteBatch &leaf_index_wb) {
    mutex_.Unlock();
    DeferCode c([this]() {
        mutex_.Lock();
    });

    PrepareCompactionTasks();
    RunCompactionTasks();
    Status s = FinishCompactionTasks();

    if (s.ok()) {
        ++num_compactions;
    }

    return s;
}

*/
Status SilkStore::DoCompactionWork(WriteBatch& leaf_index_wb) {
  Log(options_.info_log, "DoCompactionWork start\n");
  mutex_.Unlock();
  ReadOptions ro;
  ro.snapshot = leaf_index_->GetSnapshot();

  // Release snapshot after the traversal is done
  DeferCode c([&ro, this]() {
    leaf_index_->ReleaseSnapshot(ro.snapshot);
    mutex_.Lock();
  });

  std::unique_ptr<Iterator> iit(leaf_index_->NewIterator(ro));
  int self_compaction = 0;
  int num_leaves_snap = (num_leaves == 0 ? 1 : num_leaves);
  int num_splits = 0;
  iit->SeekToFirst();
  std::unique_ptr<Iterator> mit(imm_->NewIterator());
  mit->SeekToFirst();
  std::string buf, buf2;
  uint32_t run_no;
  Status s;

  GroupedSegmentAppender grouped_segment_appender(1, segment_manager_,
                                                  options_);

  Slice next_leaf_max_key;
  Slice next_leaf_index_value;
  Slice leaf_max_key;
  while (iit->Valid() && mit->Valid() && s.ok()) {
    if (next_leaf_max_key.empty()) {
      next_leaf_max_key = iit->key();
      next_leaf_index_value = iit->value();
    }

    Slice leaf_max_key = next_leaf_max_key;
    LeafIndexEntry leaf_index_entry(next_leaf_index_value);

    // Record the data read from leaf_index as well
    stats_.Add(iit->key().size() + iit->value().size(), 0);

    SegmentBuilder* seg_builder = nullptr;
    bool switched_segment = false;
    s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(
        0, &seg_builder, switched_segment);
    if (!s.ok()) return s;

    if (switched_segment &&
        leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
      // If all previous segments are built successfully and
      // the leaf_index write buffer exceeds the threshold,
      // write it down to leaf_index_ to keep the memory footprint small.
      s = leaf_index_->Write({}, &leaf_index_wb);
      if (!s.ok()) return s;
      leaf_index_wb.Clear();
    }

    uint32_t seg_id = seg_builder->SegmentId();

    assert(seg_builder->RunStarted() == false);

    int minirun_key_cnt = 0;
    // Build up a minirun of key value payloads
    while (mit->Valid() /*  && minirun_key_cnt < 1024*10 */) {
      Slice imm_internal_key = mit->key();
      ParsedInternalKey parsed_internal_key;
      if (!ParseInternalKey(imm_internal_key, &parsed_internal_key)) {
        s = Status::InvalidArgument(
            "error parsing key from immutable table during compaction");
        return s;
      }
      if (this->user_comparator()->Compare(parsed_internal_key.user_key,
                                           leaf_max_key) > 0) {
        break;
      }
      if (seg_builder->RunStarted() == false) {
        s = seg_builder->StartMiniRun();
        if (!s.ok()) {
          return s;
        }
        assert(seg_builder->RunStarted());
      }
      seg_builder->Add(mit->key(), mit->value());

      // Reading data from memtable costs no read io.
      // Record the write to segment.
      stats_.Add(0, mit->key().size() + mit->value().size());
      ++minirun_key_cnt;

      mit->Next();
    }

    stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);

    if (seg_builder->RunStarted()) {
      s = seg_builder->FinishMiniRun(&run_no);
      if (!s.ok()) {
        return s;
      }
      // Generate an index entry for the new minirun
      buf.clear();
      MiniRunIndexEntry new_minirun_index_entry = MiniRunIndexEntry::Build(
          seg_id, run_no, seg_builder->GetFinishedRunIndexBlock(),
          seg_builder->GetFinishedRunFilterBlock(),
          seg_builder->GetFinishedRunDataSize(), &buf);

      // Update the leaf index entry
      LeafIndexEntry new_leaf_index_entry;
      LeafIndexEntryBuilder::AppendMiniRunIndexEntry(
          leaf_index_entry, new_minirun_index_entry, &buf2,
          &new_leaf_index_entry);

      assert(leaf_index_entry.GetNumMiniRuns() + 1 ==
             new_leaf_index_entry.GetNumMiniRuns());
      // Write out the updated entry to leaf index
      leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
      stat_store_.UpdateLeafNumRuns(leaf_max_key.ToString(),
                                    new_leaf_index_entry.GetNumMiniRuns());
    } else {
      // Memtable has no keys intersected with this leaf
      if (leaf_index_entry.Empty()) {
        // If the leaf became empty due to self-compaction or split,
        // remove it from the leaf index
        leaf_index_wb.Delete(leaf_max_key);
        --num_leaves;
        stat_store_.DeleteLeaf(leaf_max_key.ToString());
        // fprintf(stderr, "Deleted index entry for empty leaf of key %s\n",
        // leaf_max_key.ToString().c_str());
      }
    }

    iit->Next();
    if (iit->Valid()) {
      next_leaf_max_key = iit->key();
      next_leaf_index_value = iit->value();
    }
  }
  // Memtable has keys that are greater than all the keys in leaf_index_.
  // In this case, partition the rest of memtable contents into leaves each no
  // more than options_.leaf_datasize_thresh bytes in size.
  while (s.ok() && mit->Valid()) {
    std::string buf, buf2;
    SegmentBuilder* seg_builder = nullptr;
    bool switched_segment = false;
    s = grouped_segment_appender.MakeRoomForGroupAndGetBuilder(
        0, &seg_builder, switched_segment);
    if (!s.ok()) return s;
    if (switched_segment &&
        leaf_index_wb.ApproximateSize() > kLeafIndexWriteBufferMaxSize) {
      // If all previous segments are built successfully and
      // the leaf_index write buffer exceeds the threshold,
      // write it down to leaf_index_ to keep the memory footprint small.
      s = leaf_index_->Write({}, &leaf_index_wb);
      if (!s.ok()) return s;
      leaf_index_wb.Clear();
    }

    uint32_t seg_id = seg_builder->SegmentId();

    assert(seg_builder->RunStarted() == false);
    s = seg_builder->StartMiniRun();
    if (!s.ok()) {
      fprintf(stderr, "%s", s.ToString().c_str());
      return s;
    }
    size_t bytes = 0;
    int minirun_key_cnt = 0;
    while (mit->Valid()) {
      Slice imm_internal_key = mit->key();
      ParsedInternalKey parsed_internal_key;
      if (!ParseInternalKey(mit->key(), &parsed_internal_key)) {
        s = Status::InvalidArgument(
            "error parsing key from immutable table during compaction");
        fprintf(stderr, "%s", s.ToString().c_str());
        return s;
      }
      // A leaf holds at least one key-value pair and at most
      // options_.leaf_datasize_thresh bytes of data.
      if (minirun_key_cnt > 0 &&
          bytes + imm_internal_key.size() + mit->value().size() >=
              options_.leaf_datasize_thresh * 0.95) {
        break;
      }
      bytes += imm_internal_key.size() + mit->value().size();
      leaf_max_key = parsed_internal_key.user_key;

      seg_builder->Add(imm_internal_key, mit->value());
      // Reading data from memtable costs no read io.
      // Record the write to segment.
      stats_.Add(0, mit->key().size() + mit->value().size());
      ++minirun_key_cnt;

      mit->Next();
    }
    uint32_t run_no;
    seg_builder->FinishMiniRun(&run_no);
    assert(seg_builder->GetFinishedRunDataSize());
    // Generate an index entry for the new minirun
    MiniRunIndexEntry minirun_index_entry = MiniRunIndexEntry::Build(
        seg_id, run_no, seg_builder->GetFinishedRunIndexBlock(),
        seg_builder->GetFinishedRunFilterBlock(),
        seg_builder->GetFinishedRunDataSize(), &buf);
    LeafIndexEntry new_leaf_index_entry;
    LeafIndexEntryBuilder::AppendMiniRunIndexEntry(
        LeafIndexEntry{}, minirun_index_entry, &buf2, &new_leaf_index_entry);
    leaf_index_wb.Put(leaf_max_key, new_leaf_index_entry.GetRawData());
    ++num_leaves;
    stat_store_.NewLeaf(leaf_max_key.ToString());
    stat_store_.UpdateWriteHotness(leaf_max_key.ToString(), minirun_key_cnt);
  }
  //    fprintf(stderr, "Background compaction finished, last segment %d\n",
  //    seg_id); fprintf(stderr, "avg runsize %d, self compactions %d,
  //    num_splits %d, num_leaves %d, memtable size %lu, segments size %lu\n",
  //    imm_->ApproximateMemoryUsage() / num_leaves_snap, self_compaction,
  //    num_splits, num_leaves_snap, imm_->ApproximateMemoryUsage(),
  //    segment_manager_->ApproximateSize());
  ++num_compactions;
  //        Log(options_.info_log, "Background compaction finished, last segment
  //        %d\n", seg_id);
  Log(options_.info_log,
      "avg runsize %ld, self compactions %d, num_splits %d, num_leaves %d, "
      "memtable size %lu, segments size %lu\n",
      imm_->ApproximateMemoryUsage() / num_leaves_snap, self_compaction,
      num_splits, num_leaves_snap, imm_->ApproximateMemoryUsage(),
      segment_manager_->ApproximateSize());
  return s;
}

// Perform a merge between leaves and the immutable memtable.
// Single threaded version.
void SilkStore::BackgroundCompaction() {
  auto t_start_compaction = env_->NowMicros();
  DeferCode c([this, t_start_compaction]() {
    stats_.AddTimeCompaction(env_->NowMicros() - t_start_compaction);
  });
  mutex_.Unlock();
  Status s;
  bool full_compacted = false;

  // Log(options_.info_log,"check gc %lu %lu\n",
  // segment_manager_->ApproximateSize(),
  // options_.maximum_segments_storage_size); todo GC can cause deadlock: gc is
  // ongoing and optimization_leaf is waiting for gc_mutex, and at that time
  // memtable is full.
  while (options_.maximum_segments_storage_size &&
         segment_manager_->ApproximateSize() >=
             options_.segments_storage_size_gc_threshold *
                 options_.maximum_segments_storage_size &&
         s.ok()) {
    // Log(options_.info_log, "start gc %lu %lu\n",
    // segment_manager_->ApproximateSize(),
    // options_.maximum_segments_storage_size);
    auto t_start_gc = env_->NowMicros();
    if (this->GarbageCollect() == 0) {
      // Do a full compaction to release space
      Log(options_.info_log, "full compaction\n");
      mutex_.Lock();
      s = MakeRoomInLeafLayer(true);
      mutex_.Unlock();
      full_compacted = true;

      // todo 自适应调整gc阈值
      if (options_.maximum_segments_storage_size &&
          segment_manager_->ApproximateSize() >=
              options_.segments_storage_size_gc_threshold *
                  options_.maximum_segments_storage_size) {
        size_t cur_stoage_size = segment_manager_->ApproximateSize();
        options_.maximum_segments_storage_size =
            cur_stoage_size +
            cur_stoage_size *
                (1 - options_.segments_storage_size_gc_threshold + 0.2);
      }
    }
    // Log(options_.info_log, "end gc\n");
    stats_.AddTimeGC(env_->NowMicros() - t_start_gc);
  }
  mutex_.Lock();

  if (!s.ok()) {
    bg_error_ = s;
    return;
  }

  if (full_compacted == false) {
    s = MakeRoomInLeafLayer();
    if (!s.ok()) {
      bg_error_ = s;
      return;
    }
  }

  WriteBatch leaf_index_wb;
  s = DoCompactionWork(leaf_index_wb);

  if (!s.ok()) {
    Log(options_.info_log, "DoCompactionWork failed: %s\n",
        s.ToString().c_str());
    bg_error_ = s;
  } else {
    mutex_.Unlock();
    if (leaf_index_wb.ApproximateSize()) {
      s = leaf_index_->Write({}, &leaf_index_wb);
    }
    mutex_.Lock();
    if (!s.ok()) {
      bg_error_ = s;
      Log(options_.info_log, "DoCompactionWork failed: %s\n",
          s.ToString().c_str());
      return;
    }
    // Save a new Current File
    SetCurrentFileWithLogNumber(env_, dbname_, logfile_number_);
    // Commit to the new state

    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
  }
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Status result = leveldb::DestroyDB(dbname + "/leaf_index", options);
  if (result.ok() == false) return result;
  Env* env = options.env;
  std::vector<std::string> filenames;
  result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseSilkstoreFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteFile(dbname + "/leafindex_recovery");

    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace silkstore
}  // namespace leveldb
