#include <stdexcept>
#include "nvm/nvmleafindex.h"
#include "util/coding.h"

namespace leveldb {
namespace silkstore {

 Iterator* NvmLeafIndex::NewIterator(const ReadOptions& options){
  //std::__throw_runtime_error(" NvmLeafIndex::NewIterator(const ReadOptions& options) not support\n");
  mutex_.Lock();         
  auto it =  leaf_index_->NewIterator();
  mutex_.Unlock();          

  if (it == nullptr){
    //std::cout<< "return NewEmptyIterator \n";
    return NewEmptyIterator();
  }
  //std::cout<< "return NvmLeafIndexIterator \n";
  return it;
} 


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


NvmLeafIndex::NvmLeafIndex(const Options& options, const std::string& dbname){
  cap_ = options.nvmleafindex_size; // 10ul*1204ul*1024ul*1024ul;
  const char * filename = options.nvmleafindex_file;
  std::string recovery_file = dbname + "/leafindex_recovery";
  bool file_exist = access(recovery_file.c_str(), 0) == 0;
  NvmManager *nvm_manager_ = new NvmManager(filename, cap_);
    const InternalKeyComparator internal_comparator_(leveldb::BytewiseComparator());
  leaf_index_ =  new LeafIndex(internal_comparator_, nullptr, nvm_manager_->allocate(cap_ - 50 * MB) );
  leaf_index_->Ref();
  if (file_exist){
   //   printf("leaf_index_->Recovery May exists bug \n");
      size_t seq;
      leaf_index_->Recovery(seq);
      std::cout << "recovery_file: " << recovery_file << "\n";
      printf("#### NvmLeafIndex  Recovery  Size: %ld #####\n"
                  , leaf_index_->ApproximateMemoryUsage());  
    //printf("#### NvmLeafIndex  exists #####\n");  
  }else{
      FILE *fd = fopen(recovery_file.c_str(),"w+");
      if(fd == NULL){
          printf(" recovery_file err \n");
      }
      leaf_index_->ResetCounter();
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
  //printf("NvmLeafIndex::GetSnapshot not supported\n");
//  throw std::runtime_error("NvmLeafIndex::GetSnapshot not supported");
  return nullptr;
}

void NvmLeafIndex::ReleaseSnapshot(const Snapshot* snapshot) {
  //printf("NvmLeafIndex::ReleaseSnapshot not supported\n");
  
//  throw std::runtime_error("NvmLeafIndex::ReleaseSnapshot not supported");
}

NvmLeafIndex::~NvmLeafIndex() {
  if (leaf_index_ != nullptr){
    leaf_index_->Unref();
  }
}


Status NvmLeafIndex::Write(const WriteOptions& options, WriteBatch* my_batch) {
  //throw std::runtime_error("NvmLeafIndex::Write not supported");
  if (leaf_index_->ApproximateMemoryUsage() > cap_){
    throw std::runtime_error("NvmLeafIndex out of memory\n");
  }
  mutex_.Lock();
  Status status = WriteBatchInternal::InsertInto(my_batch, leaf_index_);         
  mutex_.Unlock();          
  return status;
}


Status NvmLeafIndex::Put(const WriteOptions& options,  
             const Slice& key,
               const Slice& value) {
  std::__throw_runtime_error("NvmLeafIndex::Put not support\n");
  return Status::Corruption("");  
}

Status NvmLeafIndex::Delete(const WriteOptions& options, const Slice& key) {
  std::__throw_runtime_error("NvmLeafIndex::Delete not support\n");
  return Status::Corruption("");           
}


Status NvmLeafIndex::Get(const ReadOptions &options,
                      const Slice &key,
                      std::string *value)  {
  Status s;                        
  //printf("NvmLeafIndex::Get May Exist bug \n");
  LookupKey lkey(key, 100000000000ul);
  leaf_index_->Get(lkey, value, &s);                        
  return s;      
}


bool NvmLeafIndex::GetProperty(const Slice& property, std::string* value){
  //throw std::runtime_error("NvmLeafIndex::GetProperty not supported");
  printf("NvmLeafIndex::GetProperty not supported\n");
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