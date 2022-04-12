#include "db/dbformat.h"
#include "nvm/nvmemtable.h"
#include "port/atomic_pointer.h"

#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "nvm/nvmmanager.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include <cmath>
#include <iostream>

namespace leveldb {

namespace nvmemtable_test {

class Random {
private:
  uint32_t seed_;

public:
  explicit Random(uint32_t s) : seed_(s & 0x7fffffffu) {
    // Avoid bad seeds.
    if (seed_ == 0 || seed_ == 2147483647L) {
      seed_ = 1;
    }
  }
  uint32_t Next() {
    static const uint32_t M = 2147483647L; // 2^31-1
    static const uint64_t A = 16807;       // bits 14, 8, 7, 5, 2, 1, 0
    uint64_t product = seed_ * A;
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  uint32_t Uniform(int n) { return Next() % n; }

  bool OneIn(int n) { return (Next() % n) == 0; }
  uint32_t Skewed(int max_log) { return Uniform(1 << Uniform(max_log + 1)); }
};

leveldb::Slice RandomString(Random *rnd, int len, std::string *dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95)); // ' ' .. '~'
  }
  return leveldb::Slice(*dst);
}

std::string RandomNumberKey(Random *rnd) {
  char key[100];
  snprintf(key, sizeof(key), "%016d\n", rand() % 3000000);
  return std::string(key, 16);
}

std::string RandomString(Random *rnd, int len) {
  std::string r;
  RandomString(rnd, len, &r);
  return r;
}

void ReadWrite_TEST() {
  leveldb::DynamicFilter *dynamic_filter =
      leveldb::NewDynamicFilterBloom(1000, 0.1);
  leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
  size_t size = 10ul * GB;
  leveldb::silkstore::NvmManager *manager = new leveldb::silkstore::NvmManager(
      "/mnt/NVMSilkstore/nvmtable_test", size);
  leveldb::silkstore::Nvmem *nvm = manager->allocate(2048ul * MB);
  leveldb::NvmemTable *table = new leveldb::NvmemTable(
      cmp, dynamic_filter, nvm); // = new  silkstore::NvmemTable();
  table->Ref();
  leveldb::SequenceNumber seq = 1;
  leveldb::ValueType type = leveldb::kTypeValue;
  int N = 500000;
  std::map<std::string, std::string> m;
  for (int i = 0; i < N; i++) {
    std::string k = std::to_string(i);
    leveldb::Slice key(k);
    std::string v = std::to_string(i + 200) + "12asda3";
    m[k] = v;
    leveldb::Slice value(v);
    table->Add(seq, type, key, value);
  }
  for (int i = 0; i < N; i++) {
    auto s_key = std::to_string(i);
    leveldb::LookupKey lookupkey(leveldb::Slice(s_key), seq);
    std::string res;
    leveldb::Status status;
    bool suc = table->Get(lookupkey, &res, &status);
    if (!suc && m.count(s_key)) {
      std::cout << "can't find key " << status.ToString() << "\n";
      exit(-1);
    } else {
      if (res != m[s_key]) {
        std::cout << " find wrong value " << status.ToString() << "\n";
        exit(-1);
      }
    }
  }
  table->Unref();
  std::cout << " ## PASS Sequential READ WRITE TEST ##\n";

  dynamic_filter = leveldb::NewDynamicFilterBloom(1000, 0.1);
  nvm = manager->allocate(2048ul * MB);
  table = new leveldb::NvmemTable(cmp, dynamic_filter,
                                  nvm); // = new  silkstore::NvmemTable();
  table->Ref();
  Random rnd(0);
  m.clear();
  for (int i = 0; i < N; i++) {
    std::string k = RandomNumberKey(&rnd);
    leveldb::Slice key(k);
    std::string v = std::to_string(i + 200) + "12asda3";
    m[k] = v;
    leveldb::Slice value(v);
    table->Add(seq, type, key, value);
  }
  for (int i = 0; i < N; i++) {
    auto s_key = std::to_string(i);
    leveldb::LookupKey lookupkey(leveldb::Slice(s_key), seq);
    std::string res;
    leveldb::Status status;
    bool suc = table->Get(lookupkey, &res, &status);
    if (!suc && m.count(s_key)) {
      std::cout << "can't find key " << status.ToString() << "\n";
      exit(-1);
    } else {
      if (res != m[s_key]) {
        std::cout << " find wrong value " << status.ToString() << "\n";
        exit(-1);
      }
    }
  }

  table->Unref();
  std::cout << "  ## PASS Random READ WRITE TEST ## \n";
}

void Iterator_TEST() {

  leveldb::DynamicFilter *dynamic_filter =
      leveldb::NewDynamicFilterBloom(1000, 0.1);
  leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
  size_t size = 10ul * GB;
  leveldb::silkstore::NvmManager *manager = new leveldb::silkstore::NvmManager(
      "/mnt/NVMSilkstore/nvmtable_test", size);
  leveldb::silkstore::Nvmem *nvm = manager->allocate(2048ul * MB);
  leveldb::NvmemTable *table = new leveldb::NvmemTable(
      cmp, dynamic_filter, nvm); // = new  silkstore::NvmemTable();
  table->Ref();
  std::map<std::string, std::string> m;
  Random rnd(0);
  leveldb::SequenceNumber seq = 1;
  leveldb::ValueType type = leveldb::kTypeValue;
  int N = 500000;
  for (int i = 0; i < N; i++) {
    std::string k = RandomNumberKey(&rnd);
    leveldb::Slice key(k);
    std::string v = std::to_string(i + 200) + "12asda3";
    m[k] = v;
    leveldb::Slice value(v);
    table->Add(seq, type, key, value);
  }

  leveldb::Iterator *it = table->NewIterator();
  it->SeekToFirst();
  int counter = 0;
  auto mit = m.begin();
  while (it->Valid()) {

    // it->key() contains seq and type, which doesn't match with mit->first
    /*
    if (it->key().ToString() != mit->first){
            std::cout<< it->key().ToString() << " " << mit->first << "\n";
            std::cout << "iter wrong key counter:" << counter  << "\n";
            exit(-1);
    }
    */
    if (it->value().ToString() != mit->second) {
      std::cout << "iter wrong value counter:" << counter << "\n";
      exit(-1);
    }
    it->Next();
    mit++;
  }

  if (mit != m.end()) {
    std::cout << "iter wrong counter number:" << counter << "\n";
    exit(-1);
  }
  table->Unref();
  std::cout << "  ## PASS Iterator TEST ## \n";
}

void Delete_TEST() {

  leveldb::DynamicFilter *dynamic_filter =
      leveldb::NewDynamicFilterBloom(1000, 0.1);
  leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
  size_t size = 10ul * GB;
  leveldb::silkstore::NvmManager *manager = new leveldb::silkstore::NvmManager(
      "/mnt/NVMSilkstore/nvmtable_test", size);
  leveldb::silkstore::Nvmem *nvm = manager->allocate(2048ul * MB);
  leveldb::NvmemTable *table = new leveldb::NvmemTable(
      cmp, dynamic_filter, nvm); // = new  silkstore::NvmemTable();
  table->Ref();
  std::map<std::string, std::string> m;
  Random rnd(0);
  leveldb::SequenceNumber seq = 1;
  leveldb::ValueType type = leveldb::kTypeValue;
  int N = 500000;
  for (int i = 0; i < N; i++) {
    std::string k = std::to_string(i); // RandomNumberKey(&rnd);
    leveldb::Slice key(k);
    std::string v = std::to_string(i + 200) + "12asda3";
    m[k] = v;
    leveldb::Slice value(v);
    table->Add(seq, type, key, value);
  }

  leveldb::ValueType d_type = leveldb::kTypeDeletion;
  for (int i = 0; i < N / 20; i++) {
    std::string k = std::to_string(i); // RandomNumberKey(&rnd);
    leveldb::Slice key(k);
    std::string v = std::to_string(i + 200) + "12asda3";
    // m.erase(k);
    leveldb::Slice value(v);
    table->Add(seq, d_type, key, value);
  }

  for (int i = 0; i < N; i++) {
    auto s_key = std::to_string(i);
    leveldb::LookupKey lookupkey(leveldb::Slice(s_key), seq);
    std::string res;
    leveldb::Status status;
    bool suc = table->Get(lookupkey, &res, &status);
    if (!suc && m.count(s_key)) {
      std::cout << "can't find key " << status.ToString() << "\n";
      exit(-1);
    } else {
      if (!status.NotFound && res != m[s_key]) {
        std::cout << " find wrong value " << status.ToString() << "\n";
        exit(-1);
      }
    }
  }

  std::cout << "  ## PASS Random Delete TEST ## \n";
  table->Unref();
}

void Copy_TEST() {

  leveldb::DynamicFilter *dynamic_filter =
      leveldb::NewDynamicFilterBloom(1000, 0.1);
  leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
  size_t size = 10ul * GB;
  leveldb::silkstore::NvmManager *manager = new leveldb::silkstore::NvmManager(
      "/mnt/NVMSilkstore/nvmtable_test", size);
  leveldb::silkstore::Nvmem *nvm = manager->allocate(2048ul * MB);
  leveldb::NvmemTable *table = new leveldb::NvmemTable(
      cmp, dynamic_filter, nvm); // = new  silkstore::NvmemTable();
  table->Ref();
  std::map<std::string, std::string> m;
  Random rnd(0);
  leveldb::SequenceNumber seq = 1;
  leveldb::ValueType type = leveldb::kTypeValue;
  int N = 500000;
  for (int i = 0; i < N; i++) {
    std::string k = RandomNumberKey(&rnd);
    leveldb::Slice key(k);
    std::string v = std::to_string(i + 200) + "12asda3";
    m[k] = v;
    leveldb::Slice value(v);
    table->Add(seq, type, key, value);
  }

  leveldb::NvmemTable *imm_table = table;
  for (int i = 0; i < N; i++) {
    auto s_key = std::to_string(i);
    leveldb::LookupKey lookupkey(leveldb::Slice(s_key), seq);
    std::string res;
    leveldb::Status status;
    bool suc = imm_table->Get(lookupkey, &res, &status);
    if (!suc && m.count(s_key)) {
      std::cout << "can't find key " << status.ToString() << "\n";
      exit(-1);
    } else {
      if (res != m[s_key]) {
        std::cout << " find wrong value " << status.ToString() << "\n";
        exit(-1);
      }
    }
  }

  imm_table->Unref();
  std::cout << "  ## PASS Copy TEST ## \n";
}

void CompareMem_TEST() {

  leveldb::DynamicFilter *dynamic_filter =
      leveldb::NewDynamicFilterBloom(1000, 0.1);
  leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());

  size_t gb = GB;
  size_t size = 10 * gb;
  std::cout << "size " << size << "\n";
  leveldb::silkstore::NvmManager *manager = new leveldb::silkstore::NvmManager(
      "/mnt/NVMSilkstore/nvmtable_test", size);

  size_t asize = 50;
  asize *= MB;
  std::cout << "asize " << asize << "\n";

  leveldb::silkstore::Nvmem *nvmem = manager->allocate(asize);

  // nvm->init(4000);
  leveldb::NvmemTable *nvm = new leveldb::NvmemTable(
      cmp, dynamic_filter, nvmem); // = new  silkstore::NvmemTable();

  leveldb::MemTable *mem = new leveldb::MemTable(cmp, nullptr);
  leveldb::ValueType type = leveldb::kTypeValue;
  size_t seq = 0;

  std::map<std::string, std::string> m;

  for (int i = 0; i < 300000; i++) {
    //    std::cout<< i << "\n";
    std::string strkey = std::to_string(rand() % 10000);
    std::string strvalue = std::to_string(rand());
    leveldb::Slice key(strkey);
    leveldb::Slice value(strvalue);
    // leveldb::Slice key(std::to_string(i) );
    // leveldb::Slice value(std::to_string(i) );
    m[strkey] = strvalue;
    nvm->Add(i, type, key, value);
    mem->Add(i, type, key, value);

    // std:: cout << key.ToString()<<" " << value.ToString() << "\n";
    std::string str = std::to_string(rand());
    leveldb::LookupKey getKey(str, i);
    std::string getMem;
    std::string getNvm;
    leveldb::Status s;
    mem->Get(getKey, &getMem, &s);
    nvm->Get(getKey, &getNvm, &s);
    if (getMem != getNvm) {
      std::cout << "getMem != getImm \n";
      return;
    }
  }

  leveldb::Iterator *itnvm = nvm->NewIterator();
  leveldb::Iterator *itmem = mem->NewIterator();

  itnvm->SeekToFirst();
  itmem->SeekToFirst();

  auto mapit = m.begin();

  int count = 0;
  std::cout << "#### Test Iterator @@@@ " << m.size() << "\n";

  // while(itnvm->Valid()){
  while (mapit != m.end()) {
    /*  if (itnvm->key().ToString() != itmem->key().ToString() ){
           std::cout << "Key ont equal "<< count << "\n" ;
           return ;
     } */

    if (itnvm->value().ToString() != mapit->second) {
      std::cout << "Value ont equal \n";
      return;
    }

    itmem->Next();
    itnvm->Next();
    ++mapit;
    count++;
  }

  std::cout << "number :" << count << "\n";
}

void WriteData() {

  leveldb::DynamicFilter *dynamic_filter =
      leveldb::NewDynamicFilterBloom(1000, 0.1);
  leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
  size_t gb = GB;
  size_t size = 10 * gb;
  std::cout << "size " << size << "\n";
  leveldb::silkstore::NvmManager *manager = new leveldb::silkstore::NvmManager(
      "/mnt/NVMSilkstore/nvmtable_test", size);
  size_t asize = 50;
  asize *= MB;
  std::cout << "asize " << asize << "\n";
  leveldb::silkstore::Nvmem *nvmem = manager->allocate(asize);
  // nvm->init(4000);
  leveldb::NvmemTable *nvm = new leveldb::NvmemTable(
      cmp, dynamic_filter, nvmem); // = new  silkstore::NvmemTable();

  leveldb::ValueType type = leveldb::kTypeValue;

  for (int i = 0; i < 300; i++) {
    std::string strkey = std::to_string(i) + "yunxiao";
    std::string strvalue = std::to_string(rand()) + "du";
    leveldb::Slice key(strkey);
    leveldb::Slice value(strvalue);
    nvm->Add(i, type, key, value);
    std::cout << strkey << " ";
  }
  std::cout << "\n";
}

void Recovery() {

  leveldb::DynamicFilter *dynamic_filter =
      leveldb::NewDynamicFilterBloom(1000, 0.1);
  leveldb::InternalKeyComparator cmp(leveldb::BytewiseComparator());
  size_t gb = GB;
  size_t size = 10 * gb;
  std::cout << "size " << size << "\n";
  leveldb::silkstore::NvmManager *manager = new leveldb::silkstore::NvmManager(
      "/mnt/NVMSilkstore/nvmtable_test", size);
  size_t asize = 50;
  asize *= MB;
  std::cout << "asize " << asize << "\n";
  leveldb::silkstore::Nvmem *nvmem = manager->allocate(asize);
  // nvm->init(4000);
  leveldb::NvmemTable *nvm = new leveldb::NvmemTable(
      cmp, dynamic_filter, nvmem); // = new  silkstore::NvmemTable();
  uint64_t seq_num;
  nvm->Recovery(seq_num);
}
} // namespace nvmemtable_test
} // namespace leveldb

int main(int argc, char **argv) {

  // leveldb::nvmemtable_test::ReadWrite_TEST();
  // leveldb::nvmemtable_test::Iterator_TEST();
  // leveldb::nvmemtable_test::Delete_TEST();
  leveldb::nvmemtable_test::Copy_TEST();

  // leveldb::nvmemtable_test::WriteData();
  // leveldb::nvmemtable_test::Recovery();

  return 0;
}

//}
