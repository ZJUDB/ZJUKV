// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "nvm/nvmemtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/write_batch.h"
#include "util/coding.h"

#include <iostream>

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char *data) {
  uint32_t len;
  const char *p = data;
  p = GetVarint32Ptr(p, p + 5, &len); // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

NvmemTable::NvmemTable(const InternalKeyComparator &cmp,
                       DynamicFilter *dynamic_filter, silkstore::Nvmem *nvmem)
    : comparator_(cmp), refs_(0), num_entries_(0), searches_(0),
      dynamic_filter(dynamic_filter), nvmem(nvmem), counters_(0),
      memory_usage_(0), dram_usage_(0) {}

NvmemTable::~NvmemTable() {
  assert(refs_ == 0);
  if (dynamic_filter) {
    delete dynamic_filter;
    dynamic_filter = nullptr;
  }
  if (nvmem) {
    delete nvmem;
    nvmem = nullptr;
  }
}

size_t NvmemTable::Searches() const { return searches_; }
size_t NvmemTable::NumEntries() const { return num_entries_; }
size_t NvmemTable::ApproximateMemoryUsage() {
  return memory_usage_;
} // arena_.MemoryUsage(); }

int NvmemTable::KeyComparator::operator()(const char *aptr,
                                          const char *bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char *EncodeKey(std::string *scratch, const Slice &target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}
class NvmemTableIterator : public Iterator {
public:
  explicit NvmemTableIterator(NvmemTable::Index *index) : index(index) {
    iter_ = index->begin();
  }
  virtual bool Valid() const { return iter_ != index->end() && iter_->second; }
  // Seek 中的key 带有 8bits的序列号和标记位
  virtual void Seek(const Slice &k) {
    int k_len = k.size();
    iter_ = index->lower_bound(k.ToString().substr(0, k_len - 8));
  }
  virtual void SeekToFirst() { iter_ = index->begin(); }
  virtual void SeekToLast() {
    iter_ = index->end();
    iter_--;
    fprintf(stderr, "MemTableIterator's SeekToLast() is not implemented !");
    // assert(true);
  }
  virtual void Next() { ++iter_; }
  virtual void Prev() {
    --iter_;
    fprintf(stderr, "MemTableIterator's Prev() is not implemented ! \n");
    //  sleep(111);
    //  assert(true);
  }
  virtual Slice key() const {
    return GetLengthPrefixedSlice((char *)(iter_->second));
  }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice((char *)(iter_->second));
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

private:
  NvmemTable::Index *index;
  NvmemTable::Index::iterator iter_;

  // No copying allowed
  NvmemTableIterator(const NvmemTableIterator &);
  void operator=(const NvmemTableIterator &);
};

Iterator *NvmemTable::NewIterator() { return new NvmemTableIterator(&index_); }

Status NvmemTable::AddCounter(size_t added) {
  counters_ += added;
  nvmem->UpdateCounter(counters_);
  return Status::OK();
}

size_t NvmemTable::GetCounter() { return nvmem->GetCounter(); }

Status NvmemTable::AddBatch(const WriteBatch *batch) {
  /* int64_t offset = nvmem->Insert(batch->buf, batch->offset_);
  int nums = batch->offset_arr_.size();
//  std::cout<< "batch size :" << batch->offset_arr_.size()<< "\n";
  for(int i = 0; i < nums; i++){
      uint64_t address = offset + batch->offset_arr_[i].second;
      index_.insert(batch->offset_arr_[i].first, address);
   //   std::cout<< "key :" << batch->offset_arr_[i].first<< "\n";
      if (dynamic_filter)
        dynamic_filter->Add(batch->offset_arr_[i].first);
  }
  AddCounter(nums);
  num_entries_ += nums;
  memory_usage_ += batch->offset_; */
  return Status::OK();
}

bool NvmemTable::AddIndex(Slice key, uint64_t val) {
  index_[key.ToString()] = val;
  return true;
}

Status NvmemTable::Recovery(SequenceNumber &max_sequence) {
  // ToDo Get the right counters
  // Because updatecounter is not called in testcase, counters is set to 20
  int counters = nvmem->GetCounter();
  uint64_t offset = 16;
  uint64_t address = nvmem->GetBeginAddress();
  uint32_t key_length;
  uint32_t value_length;
  counters_ = counters;
  if (counters > 0) {
    const char *key_ptr =
        GetVarint32Ptr((char *)(address + offset),
                       (char *)(address + offset + 5), &key_length);
    std::string key = // Slice(key_ptr, key_length - 8).ToString();
        std::string(key_ptr, key_length - 8);
    max_sequence = SequenceNumber(DecodeFixed64(key_ptr + key_length - 8));
    max_sequence = (max_sequence >> 8) + counters;
    // std::cout << "max_sequence: " << max_sequence << "\n";
  }

  while (counters--) {
    const char *key_ptr =
        GetVarint32Ptr((char *)(address + offset),
                       (char *)(address + offset + 5), &key_length);
    std::string key = // Slice(key_ptr, key_length - 8).ToString();
        std::string(key_ptr, key_length - 8);
    AddIndex(key, address + offset);
    offset += key_length + VarintLength(key_length);
    const char *value_ptr =
        GetVarint32Ptr((char *)(key_ptr + key_length),
                       (char *)(key_ptr + key_length + 5), &value_length);
    offset += value_length + VarintLength(value_length);
  }
  nvmem->UpdateIndex(offset);
  memory_usage_ = offset;
  // printf("Recovery Finished ! \n");
  return Status::OK();
}

void NvmemTable::Add(SequenceNumber s, ValueType type, const Slice &key,
                     const Slice &value) {
  // Format of an entry is concatenation of:
  //  magic number
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char *p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  uint64_t address = nvmem->Insert(buf, encoded_len);
  index_[key.ToString()] = address;
  if (dynamic_filter) {
    dynamic_filter->Add(key);
  }
  ++num_entries_;
  // update memory_usage_ to recode nvm's usage size
  memory_usage_ += encoded_len;
}

bool NvmemTable::Get(const LookupKey &key, std::string *value, Status *s) {
  if (dynamic_filter != nullptr && !dynamic_filter->KeyMayMatch(key.user_key()))
    return false;
  ++searches_;
  Slice memkey = key.user_key();
  uint64_t address = 0;
  bool suc = index_.count(memkey.ToString());

  if (suc) {
    /*  Slice foundkey = NvmGetLengthPrefixedSlice((char *)(address));
     std::cout << "found ! key: "<< foundkey.ToString() <<"\n"; */
    // entry format is:
    //    magicNum
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    address = index_[memkey.ToString()];
    uint32_t key_length;
    const char *key_ptr =
        GetVarint32Ptr((char *)(address), (char *)(address + 5),
                       &key_length); //
                                     //  +5: we assume "p" is not corrupted
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
      case kTypeValue: {
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        value->assign(v.data(), v.size());
        return true;
      }
      case kTypeDeletion:
        *s = Status::NotFound(Slice());
        return true;
      }
    } else {
      std::runtime_error(" can't find key value \n");
    }
  }
  return false;
}
} // namespace leveldb
