#include <stdexcept>
#include "nvm/nvmleafindex.h"
#include "util/coding.h"

namespace leveldb {
namespace silkstore {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}



class NvmLeafIndexIterator: public Iterator {
 public:
  explicit NvmLeafIndexIterator(NvmLeafIndex::Index* index) : index(index) { 
    iter_ = index->begin();
  }
  virtual bool Valid() const { return iter_ != index->end() && iter_->second ; }
  virtual void Seek(const Slice& k) { iter_ = index->lower_bound(k.ToString()); }
  virtual void SeekToFirst() { iter_ = index->begin(); }
  virtual void SeekToLast() {  
      fprintf(stderr, "MemTableIterator's SeekToLast() is not implemented !");  
      assert(true);
  }
  virtual void Next() {  ++iter_; }
  virtual void Prev() { --iter_;
    /* iter_.Prev(); */ 
      fprintf(stderr, "MemTableIterator's Prev() is not implemented ! \n");  
      sleep(111);
      assert(true);
  }
  virtual Slice key() const {  
     return iter_->first;//GetLengthPrefixedSlice((char *)(iter_->second)); 
  }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice((char *)(iter_->second));
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  NvmLeafIndex::Index* index;
  NvmLeafIndex::Index::iterator iter_;

  // No copying allowed
  NvmLeafIndexIterator(const NvmLeafIndexIterator&);
  void operator=(const NvmLeafIndexIterator&);
};



class EmptyKVIterator: public Iterator {
public:
  virtual void Seek(const std::string &key) {};

  virtual void SeekToFirst(){};

  virtual void SeekToLast(){};

  virtual bool Valid(){return false;};

  virtual void Next() {};

  virtual void Prev(){};

  virtual std::string Key(){return nullptr;};

  virtual std::string Value(){return nullptr;};
};


 Iterator* NvmLeafIndex::NewIterator(const ReadOptions& options){
    //std::__throw_runtime_error(" NvmLeafIndex::NewIterator(const ReadOptions& options) not support\n");
    if (leaf_index_->size() == 0){
       std::cout<< "return NewEmptyIterator \n";
       return NewEmptyIterator();
    }
    
    std::cout<< "leaf_index_->size()" <<leaf_index_->size()  <<  "\n";

    return new NvmLeafIndexIterator(leaf_index_);
} 

bool NvmLeafIndex::AddIndex(Slice key ,uint64_t val){
    (*leaf_index_)[key.ToString()] = val;
    return true;
}


bool NvmLeafIndex::Recovery(){
  // ToDo Get the right counters
  // Because updatecounter is not called in testcase, counters is set to 20
  int counters = nvmem_->GetCounter();
  uint64_t offset = 16;
  uint64_t address = nvmem_->GetBeginAddress();
  uint32_t key_length;
  uint32_t value_length;
  counters_ = counters;
  if (counters > 0){
    const char* key_ptr = GetVarint32Ptr((char *) (address + offset), 
        (char *) (address + offset + 5), &key_length);
    std::string key = // Slice(key_ptr, key_length - 8).ToString();
                      std::string(key_ptr, key_length - 8);
    // std::cout << "max_sequence: " << max_sequence << "\n";
  }

  while(counters-- ){
    const char* key_ptr = GetVarint32Ptr((char *) (address + offset), 
        (char *) (address + offset + 5), &key_length);
    std::string key = // Slice(key_ptr, key_length - 8).ToString();
                      std::string(key_ptr, key_length - 8);
    AddIndex(key, address + offset);
    offset += key_length +  VarintLength(key_length); 
    const char* value_ptr = GetVarint32Ptr((char *) (key_ptr  + key_length), 
       (char *) (key_ptr  + key_length + 5), &value_length);
    offset += value_length +  VarintLength(value_length); 

  }
  nvmem_->UpdateIndex(offset);
  //printf("Recovery Finished ! \n");
  return true;
}


NvmLeafIndex::NvmLeafIndex(const Options& options, const std::string& dbname){
  counters_ = 0;
  leaf_index_ = new Index();
  cap_ = options.nvmleafindex_size; // 10ul*1204ul*1024ul*1024ul;
  const char * filename = options.nvmleafindex_file;
  std::string recovery_file = dbname + "/leafindex_recovery";
  bool file_exist = access(recovery_file.c_str(), 0) == 0;
  NvmManager *nvm_manager_ = new NvmManager(filename, cap_);
    const InternalKeyComparator internal_comparator_(leveldb::BytewiseComparator());

  nvmem_ = nvm_manager_->allocate(cap_ - 50 * MB);

  if (file_exist){
      Recovery();
      printf("#### NvmLeafIndex  Recovery #####\n");  
  }else{
      FILE *fd = fopen(recovery_file.c_str(),"w+");
      if(fd == NULL){
          printf(" recovery_file err \n");
      }
      printf("#### Create NvmLeafIndex  #####\n");  
  }
}

Status NvmLeafIndex::OpenNvmLeafIndex(const Options& options,
                     const std::string& name,
                     DB** dbptr){
  *dbptr = new NvmLeafIndex(options, name);
  return Status::OK();
}


const Snapshot* NvmLeafIndex::GetSnapshot() {
  throw std::runtime_error("NvmLeafIndex::GetSnapshot not supported");
}

void NvmLeafIndex::ReleaseSnapshot(const Snapshot* snapshot) {
  throw std::runtime_error("NvmLeafIndex::ReleaseSnapshot not supported");
}

NvmLeafIndex::~NvmLeafIndex() {
  if (leaf_index_ != nullptr){
    delete leaf_index_;
  }
}


Status NvmLeafIndex::Write(const WriteOptions& options, WriteBatch* my_batch) {
  throw std::runtime_error("NvmLeafIndex::Write not supported");
  /* if (leaf_index_->ApproximateMemoryUsage() > cap_){
    throw std::runtime_error("NvmLeafIndex out of memory\n");
  }
  mutex_.Lock();
  Status status = WriteBatchInternal::InsertInto(my_batch, leaf_index_);         
  mutex_.Unlock();    */       
  return Status::OK(); 
}


bool NvmLeafIndex::AddCounter(size_t added){
  counters_ += added;
  nvmem_->UpdateCounter(counters_);
  return true;
}

Status NvmLeafIndex::Put(const WriteOptions& options,  
             const Slice& key,
               const Slice& value) {

    size_t key_size = key.size();
    size_t val_size = value.size();
   // std::cout << "PUT:" << key.ToString() << " value: " << value.ToString() << "\n";
    size_t internal_key_size = key_size + 8;
    const size_t encoded_len =
    VarintLength(internal_key_size) + internal_key_size +
    VarintLength(val_size) + val_size;
    char* p = EncodeVarint32(buf, internal_key_size);
    memcpy(p, key.data(), key_size);
    p += key_size;
    size_t s = 0;
    EncodeFixed64(p, (s << 8) | kTypeValue);
    p += 8;
    p = EncodeVarint32(p, val_size);
    memcpy(p, value.data(), val_size);
    assert(p + val_size == buf + encoded_len);
    uint64_t address = nvmem_->Insert(buf, encoded_len);
    
    mutex_.lock();
    (*leaf_index_)[key.ToString()] = address;
    AddCounter(1);
    mutex_.unlock();

    return Status::OK();  
}

Status NvmLeafIndex::Delete(const WriteOptions& options, const Slice& key) {
    std::lock_guard<std::mutex> lock(mutex_); 
    leaf_index_->erase(key.ToString());
    return Status::OK();          
}


// Get key-value equal or less than target key
Status NvmLeafIndex::Get(const ReadOptions &options,
                      const Slice &key,
                      std::string *value)  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = leaf_index_->lower_bound(key.ToString());

    Slice key_slice = GetLengthPrefixedSlice((char *)(it->second));
    *value = GetLengthPrefixedSlice(key_slice.data() + key_slice.size()).ToString();

    std::cout <<"Get value:" <<*value  << "\n";
    //*value = it->second;                      
    return Status::OK();        
}



bool NvmLeafIndex::GetProperty(const Slice& property, std::string* value){
  throw std::runtime_error("NvmLeafIndex::GetProperty not supported");
  printf("NvmLeafIndex::GetProperty not supported");
  return true;
}
void NvmLeafIndex::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  throw std::runtime_error("NvmLeafIndex::GetApproximateSizes not supported");
}
void NvmLeafIndex::CompactRange(const Slice* begin, const Slice* end) {
  throw std::runtime_error("NvmLeafIndex::CompactRange not supported");
}

}
}