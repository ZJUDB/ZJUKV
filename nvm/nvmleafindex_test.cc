#include"nvm/nvmleafindex.h"



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
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    uint64_t product = seed_ * A;
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  uint32_t Uniform(int n) { return Next() % n; }

  bool OneIn(int n) { return (Next() % n) == 0; }
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

leveldb::Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
  }
  return leveldb::Slice(*dst);
}

std::string RandomNumberKey(Random* rnd) {
    char key[100];
    snprintf(key, sizeof(key), "%016d\n", rand() % 3000000);
    return std::string(key, 16);
}

std::string RandomString(Random* rnd, int len) {
    std::string r;
    RandomString(rnd, len, &r);
    return r;
}

 

void SequentialWrite(){
  
        leveldb::DB* db_ = nullptr;
       // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
        leveldb::Status s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "", &db_);
        assert(s.ok()==true);
        std::cout << " ######### SequentialWrite Test ######## \n";
        static const int kNumOps = 30;
        static const long int kNumKVs = 50;
        static const int kValueSize = 36000;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = RandomNumberKey(&rnd);
        }
       // sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Sequential Insert And Get Test ######## \n";

        clock_t startTime,endTime;


        startTime = clock();
        leveldb::WriteBatch batch;
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                batch.Put(key,value);
                db_->Write(leveldb::WriteOptions(),&batch);
                std::cout<< "insert: " << key << " " << value.size() << "\n";
                m[key] = value;          
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
               // std::cout<< "get: " << key << " " << res << "\n";                
                if (res != value){
                   fprintf(stderr, "Key %s has wrong value %s \n",key.c_str(), res.c_str() );
                   return ;
                }
                batch.Clear(); 
        }
        endTime = clock();
        std::cout << "The Insert time is: " <<(endTime - startTime) << "\n";

        std::cout << " @@@@@@@@@ PASS #########\n";
        std::cout << " ######### Begin Sequential Get Test ######## \n";

        startTime = clock();
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
                auto ans = m[key];
                if (res != ans){
                   fprintf(stderr, "Key %s has wrong value %s \n",key.c_str(), res.c_str() );
                   return ;
                }
        }
        endTime = clock();
        std::cout << "The Get time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";



        std::cout << " ######### Begin Slice Iterator Test ######## \n";
        startTime = clock();        
        leveldb::Iterator* sit = db_->NewIterator(leveldb::ReadOptions());
        sit->SeekToFirst();
        auto mit = m.begin();
        int count = 0;
        while (mit != m.end() && sit->Valid()) {
            auto res_key = sit->key();
            auto res_value = sit->value();
            auto ans_key = mit->first;
            auto ans_value = mit->second;
        //    std::cout << res_key.ToString() << " " << ans_key << "\n"; 
        //    std::cout << res_value.ToString() << " " << ans_value << "\n";        
                   
            assert(res_key == ans_key);
            assert(res_value == ans_value);
            sit->Next();
            ++mit;
            count++;
        }
        
        endTime = clock();
        std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n";
 


        /* std::cout << " ######### Begin Sequential Iterator Test ######## \n";
        startTime = clock();        
        auto it = db_->NewIterator();
        it->SeekToFirst();
        auto mit = m.begin();
        count = 0;
        while (mit != m.end() && it->Valid()) {
            auto res_key = it->Key();
            auto res_value = it->Value();
            auto ans_key = mit->first;
            auto ans_value = mit->second;
        //    std::cout << res_key.ToString() << " " << ans_key << "\n"; 
        //    std::cout << res_value.ToString() << " " << ans_value << "\n";        
                   
            assert(res_key == ans_key);
            assert(res_value == ans_value);
            it->Next();
            ++mit;
            count++;
        }
        
        endTime = clock();
        std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n"; */
 
        std::cout << " @@@@@@@@@ PASS #########\n";
        delete db_;
        std::cout << " Delete Open Db \n";
}


void  WriteBatchTest(){

        leveldb::DB* db_ = nullptr;
       // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
        leveldb::Status s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "", &db_);
        assert(s.ok()==true);
        std::cout << " ######### Bench Test ######## \n";
        static const int kNumOps = 300000;
        static const long int kNumKVs = 500000;
        static const int kValueSize = 100*100;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = RandomNumberKey(&rnd);
        }
        sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Sequential Insert And Get Test ######## \n";

        clock_t startTime,endTime;
        startTime = clock();
        leveldb::WriteBatch batch;
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                batch.Clear();
                batch.Put(key, value);

                db_->Write(leveldb::WriteOptions(), &batch);
               // std::cout<< "insert: " << key << " " << value << "\n";
                m[key] = value;          
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
               // std::cout<< "get: " << key << " " << res << "\n";                
                if (res != value){
                   fprintf(stderr, "Key %s has wrong value %s \n",key.c_str(), res.c_str() );
                   return ;
                } 
        }
        endTime = clock();
        std::cout << "The Insert time is: " <<(endTime - startTime) << "\n";

        std::cout << " @@@@@@@@@ PASS #########\n";
        std::cout << " ######### Begin Sequential Get Test ######## \n";

        startTime = clock();
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
                auto ans = m[key];
                if (res != ans){
                   fprintf(stderr, "Key %s has wrong value %s \n",key.c_str(), res.c_str() );
                   return ;
                }
        }
        endTime = clock();
        std::cout << "The Get time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";


        std::cout << " ######### Begin Sequential Iterator Test ######## \n";
        startTime = clock();        
        auto it = db_->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        auto mit = m.begin();
        int count = 0;
        while (mit != m.end() && it->Valid()) {
            auto res_key = it->key();
            auto res_value = it->value();
            auto ans_key = mit->first;
            auto ans_value = mit->second;
        //    std::cout << res_key.ToString() << " " << ans_key << "\n"; 
        //    std::cout << res_value.ToString() << " " << ans_value << "\n";        
                   
            assert(res_key == ans_key);
            assert(res_value == ans_value);
            it->Next();
            ++mit;
            count++;
        }
        
        endTime = clock();
        std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n";
 
        std::cout << " @@@@@@@@@ PASS #########\n";
        delete db_;
        std::cout << " Delete Open Db \n";



         
}



void Bench(){
  
        leveldb::DB* db_ = nullptr;
       // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
        leveldb::Status s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "", &db_);
        assert(s.ok()==true);
        std::cout << " ######### Bench Test ######## \n";
        static const int kNumOps = 1000;
        static const long int kNumKVs = 3000;
        static const int kValueSize = 2048;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = RandomNumberKey(&rnd);
        }
        //sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Bench Insert ######## \n";

        clock_t startTime,endTime;
        startTime = clock();
        leveldb::WriteBatch batch;
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                batch.Clear();
                batch.Put(key, value);
                db_->Write(leveldb::WriteOptions(),&batch);
        }
        endTime = clock();
        std::cout << "The Insert time is: " <<(endTime - startTime) << "\n";

        std::cout << " @@@@@@@@@ PASS #########\n";
        /* 
        std::cout << " ######### Begin Sequential Get Test ######## \n";

        startTime = clock();
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
        }
        endTime = clock();
        std::cout <<"kNumOps: " << kNumOps << " The Get time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";
 */
        std::cout << " ######### Begin Sequential Iterator Test ######## \n";
        startTime = clock(); 

        for (int i = 0; i < kNumOps; i++) {
            auto it = db_->NewIterator(leveldb::ReadOptions());
            it->Seek(keys[i % kNumKVs]);
            auto val = it->value();
            //std::cout<<val.size() << "\n";
        }       
        /*        
        auto it = db_->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        int counter = 0;
       // std::cout << "it->Valid() " << (it->Valid() == true )<< "\n";
        while ( it->Valid()) {
          //  std::cout << counter++;
            auto res_key = it->key();
           // auto res_value = it->value();
            it->Next();
        } */
        endTime = clock();
        std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";
        delete db_;
        std::cout << " Delete Open Db \n";
}




void WriteBatchBench(){
  
        leveldb::DB* db_ = nullptr;
       // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
        leveldb::Status s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "", &db_);
        assert(s.ok()==true);
        std::cout << " ######### Bench Test ######## \n";
        static const int kNumOps = 300000;
        static const long int kNumKVs = 500000;
        static const int kValueSize = 36000;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = RandomNumberKey(&rnd);
        }
        //sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Bench Insert Test ######## \n";
        clock_t startTime,endTime;
        startTime = clock();
        leveldb::WriteBatch batch;

        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                batch.Clear();
                batch.Put(key, value);
                db_->Write(leveldb::WriteOptions(),&batch); 
        }
        endTime = clock();
        std::cout << "The Insert time is: " <<(endTime - startTime) << "\n";

        std::cout << " @@@@@@@@@ PASS #########\n";
        std::cout << " ######### Begin Bench Get Test ######## \n";

        startTime = clock();
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
        }
        endTime = clock();
        std::cout << "The Get time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";

        std::cout << " ######### Begin Bench Iterator Test ######## \n";
        startTime = clock();        
        auto it = db_->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        while (it->Valid()) {
            auto res_key = it->key();
            auto res_value = it->value();
            
            it->Next();
        }
        endTime = clock();
        std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n";
 
        std::cout << " @@@@@@@@@ PASS #########\n";
        delete db_;
        std::cout << " Delete Open Db \n";
}
/* 

void IterTest(){


    leveldb::DB* db_ = nullptr;
    // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
    leveldb::Status s = leveldb::silkstore::NvmemLeafIndex::OpenLeafIndex(leveldb::Options(), "", &db_);
    assert(s.ok()==true);
    std::cout << " ######### IterTest  ######## \n";
    static const int kNumOps = 300;
    static const long int kNumKVs = 500;
    static const int kValueSize = 36000;

    Random rnd(0);
    std::vector<std::string> keys(kNumKVs);
    for (int i = 0; i < kNumKVs; ++i) {
            keys[i] = RandomNumberKey(&rnd);
    }
    //sort(keys.begin(), keys.end());
    std::map<std::string, std::string> m;
    std::cout << " ######### Begin IterTest ######## \n";
    clock_t startTime,endTime;
    startTime = clock();
    leveldb::WriteBatch batch;

    for (int i = 0; i < kNumOps; i++) {
            std::string key = keys[i % kNumKVs];
            std::string value = RandomString(&rnd, kValueSize); //std::to_string(i+10);
          //  batch.Clear();
          //  batch.Put(key, value);
            db_->Write(leveldb::WriteOptions(),&batch); 
            std::cout << "insert key value: " << key << "\n";
            //db_->Put(key, value);

            m[key] = value;

          //  auto it = db_->NewIterator(leveldb::ReadOptions());
                //std::cout << "insert key value: " << key << "->" << value << "\n";
            auto it = db_->NewIterator();
            it->SeekToFirst();
            auto mit = m.begin();
            while (it->Valid()){
                    std::string key = it->Key();
                    // std::cout<<" IterTest key : "<< key << "\n";
                    if (key != mit->first){
                    std::cout<< "it->key() != mit->first err\n ";
                    std::cout<< " it->key: " << key << "\n";
                    std::cout <<"mit->key: "<< mit->first << "\n";
                    return ;
                    }

                    if (it->Value() != mit->second){
                    std::cout<< "it->value() != mit->value err\n ";
                    std::cout<< " it->value: " << it->Value() << "\n";
                    std::cout <<"mit->value: "<< mit->second << "\n";
                    return ;
                    }
                    it->Next();
                    mit++;
            }  
            
    }

    auto it = db_->NewIterator(leveldb::ReadOptions());
    while (it->Valid()){
            std::cout<< "it->key() " << it->key().ToString();
            std::cout<< " it->value: " << it->value().ToString() << "\n";
            it->Next();
    }


    std::cout << " @@@@@@@@@ PASS #########\n";
    delete db_;
    std::cout << " Delete Open Db \n";


}
 */


void GetBench(){


}


void IterBench(){
  
        leveldb::DB* db_ = nullptr;
       // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
        leveldb::Status s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "", &db_);
        assert(s.ok()==true);
        std::cout << " ######### IterBench Test ######## \n";
        static const int kNumOps = 300000;
        static const long int kNumKVs = 500000;
        static const int kValueSize = 36000;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = RandomNumberKey(&rnd);
        }
        //sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Bench Insert Test ######## \n";
        clock_t startTime,endTime;
        startTime = clock();
        leveldb::WriteBatch batch;

        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = RandomString(&rnd, kValueSize);
                batch.Clear();
                batch.Put(key, value);
                db_->Write(leveldb::WriteOptions(),&batch); 
        }
        endTime = clock();
        std::cout << "The Insert time is: " <<(endTime - startTime) << "\n";

        std::cout << " @@@@@@@@@ PASS #########\n";
        std::cout << " ######### Begin Bench Get Test ######## \n";

        startTime = clock();
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
        }
        endTime = clock();
        std::cout << "The Get time is: " <<(endTime - startTime) << "\n";
        std::cout << " @@@@@@@@@ PASS #########\n";

        std::cout << " ######### Begin Bench Iterator Test ######## \n";
        startTime = clock();        
        auto it = db_->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        while (it->Valid()) {
            auto res_key = it->key();
            auto res_value = it->value();
            
            it->Next();
        }
        endTime = clock();
        std::cout << "The Iterator time is: " <<(endTime - startTime) << "\n";
 
        std::cout << " @@@@@@@@@ PASS #########\n";
        delete db_;
        std::cout << " Delete Open Db \n";
}


void EmptyIter(){
        leveldb::DB* db_ = nullptr;

       // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
        leveldb::Status s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "./nvm_leaf_test", &db_);
        assert(s.ok()==true);
      
        std::cout << " ######### Begin Empty Iterator Test ######## \n";
        auto it = db_->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        int count = 0;
        while (it->Valid()) {
            auto res_key = it->key();
            auto res_value = it->value();
            it->Next();
            count++;
        }
        std::cout << " @@@@@@@@@ PASS #########\n";
        delete db_;
        std::cout << " Delete Open Db \n";
}

void Recovey(){
        leveldb::DB* db_ = nullptr;
       // leveldb::silkstore::NvmemLeafIndex* db = new NvmemLeafIndex(Options(), nullptr);
        leveldb::Options ops;
        ops.nvmleafindex_file = "/mnt/NVMSilkstore/nvm_leaf_test";
        ops.nvmleafindex_size = 3600 * 400;
        leveldb::Status s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "./nvm_leaf_test", &db_);
        assert(s.ok()==true);
        std::cout << " ######### Recovey Test ######## \n";
        static const int kNumOps = 30;
        static const long int kNumKVs = 100;
        static const int kValueSize = 3600;

        Random rnd(0);
        std::vector<std::string> keys(kNumKVs);
        for (int i = 0; i < kNumKVs; ++i) {
                keys[i] = std::to_string(i+10);//RandomNumberKey(&rnd);
        }
       // sort(keys.begin(), keys.end());
        std::map<std::string, std::string> m;
        std::cout << " ######### Begin Sequential Insert And Get Test ######## \n";
        leveldb::WriteBatch batch;
        for (int i = 0; i < kNumOps; i++) {
                std::string key = keys[i % kNumKVs];
                std::string value = std::to_string(i*10 + 15);//RandomString(&rnd, kValueSize);
                std::cout<< "insert: " << key << " value " << value << "\n";
                batch.Put(key, value);
                db_->Write(leveldb::WriteOptions(),&batch);
                m[key] = value;          
                std::string res;
                s = db_->Get(leveldb::ReadOptions(), key, &res);
               // std::cout<< "get: " << key << " " << res << "\n";                
                if (res != value){
                   fprintf(stderr, "Key %s has wrong value %s \n",key.c_str(), res.c_str() );
                   return ;
                }
                batch.Clear(); 
        }
        delete db_;

        std::cout << " ######### Recovery Test ######## \n";
        s = leveldb::silkstore::NvmLeafIndex::OpenNvmLeafIndex(leveldb::Options(), "./nvm_leaf_test", &db_);
        
        auto it = db_->NewIterator(leveldb::ReadOptions());
        it->SeekToFirst();
        int count = 0;
        while (it->Valid()) {
            auto res_key = it->key();
            auto res_value = it->value();
            it->Next();
            //std::cout << "res_key: " << res_key.ToString() << " res_value: " << res_value.ToString() << "\n";
            count++;
        }
        std::cout << "kNumOps: " << kNumOps << " count " << count <<"\n";

}

int main(int argc, char const *argv[]){
    
    //IterTest();
   // EmptyIter();
    Recovey();
    //WriteBatchTest();
    //SequentialWrite();

   // WriteBatchBench();
   // Bench();
    return 0;
}


  


