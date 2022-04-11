#include<map>
#include <sys/time.h>
#include <iostream>
using namespace std;

static int op_nums = 10000000;




void GetFunc1( map<int, int> & m) {
    int key;
    int value;
    for(int i = 0; i < op_nums; i++){
        key = i; //rand();
        if (m.count(key)){
            value = m[key];
        }
    }
}


void GetFunc2( map<int, int> & m) {
    int key;
    int value;
    for(int i = 0; i < op_nums; i++){
        key = i;//rand();        
        auto it = m.find(key);
        if (it != m.end()){
            value = it->second;
        }
    }
}


int main(int argc, char const *argv[]) {
    
    map<int, int> m;
    for (int i = 0; i < op_nums; i++) {
        int key = i; // rand();
        int value = rand();
        m[key] = value;
    }


    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);

    GetFunc1(m);

    gettimeofday(&t2,NULL);
    timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000000.0;

    std::cout << "Run time's :" << timeuse <<"\n";
    return 0;
}
