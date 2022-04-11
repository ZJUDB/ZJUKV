/**
 * @ Author: Yunxiao Du
 * @ Create Time: 2021-05-20 20:19:30
 * @ Description: Using nvm and dram to replace leveldb's oringinal memtable
 */

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LeafIndex_STL_H_
#define STORAGE_LEVELDB_DB_LeafIndex_STL_H_


#include <string>
#include <map>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "leveldb/filter_policy.h"

//#include "leveldb/nvm_write_batch.h"

#include "nvm/nvmem.h"

namespace leveldb {


class InternalKeyComparator;
class MemTableIterator;

class LeafIndex {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  //explicit LeafIndex(const InternalKeyComparator& comparator, 
  //    DynamicFilter * dynamic_filter, silkstore::Nvmem *nvmem, silkstore::NvmLog *nvmlog);
  explicit LeafIndex(const InternalKeyComparator& comparator, 
      DynamicFilter* dynamic_filter, silkstore::Nvmem *nvmem);

  // Increase reference count.
  void Ref() {
    ++refs_;  
    return ;
  }

  void print(){
    nvmem->print();
  }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
     // printf("release LeafIndex\n");     
      delete this;
    }
    return ;
  }
  size_t Size(){
    return index_.size();
  }
  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTable is being modified.
  size_t ApproximateMemoryUsage();
  // Return an iterator that yields the contents of the memtable.
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();
  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value);
  Status AddBatch(const WriteBatch* b);
  Status ResetCounter();
  Status Recovery(SequenceNumber& max_sequence);
  Status AddCounter(size_t added);
  size_t GetCounter();
  bool AddIndex(Slice, uint64_t);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);
  size_t NumEntries() const;
  size_t Searches() const;
 private:
  ~LeafIndex();  // Private since only Unref() should be used to delete it
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    int operator()(const char* a, const char* b) const;
  };
  friend class LeafIndexIterator;
  friend class LeafIndexBackwardIterator;

  typedef std::map<std::string,uint64_t> Index;
  KeyComparator comparator_;
  int refs_;
  Index index_;
  silkstore::Nvmem *nvmem;
  char buf[1024ul*1024ul*16ul];
  size_t num_entries_;
  size_t searches_;
  size_t counters_;
  size_t memory_usage_;
  // Using for debug
  size_t dram_usage_;
  DynamicFilter * dynamic_filter;
  // No copying allowed
  LeafIndex(const LeafIndex&);
  void operator=(const LeafIndex&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
