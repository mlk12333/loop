//
//  main.cpp
//  Loop_load_data
//
//  Created by 孟令凯 on 2021/9/28.
//

#include <iostream>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <chrono>
#include <atomic>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <cassert>

using namespace std;

const int nthread = 4;      // 线程数
//const int maxs = 10000000;   // 最大环数
const int maxn = 1000000;    // 最大节点数

// 数据读取buffer
uint64_t loadbuf_edge[nthread][maxn][3];
uint64_t loadbuf_v[nthread][maxn];
double loadbuf_money[nthread][maxn];
uint32_t loadnum[nthread];

//// 图数据结构（前向星）
//struct Edge{
//    uint32_t id;
//    uint32_t money;
//};
//
//Edge From[maxn];        // 正图
//Edge To[maxn];          // 反图
//uint32_t phead[maxn];   // 各id起始位置
//uint32_t plen[maxn];    // 各id转账数
//uint32_t qhead[maxn];
//uint32_t qlen[maxn];
//
//// 简易的固定长度vector，效率和数组相近
//class Vector{
//public:
//    Vector(){ _data = new uint32_t[maxn]; }
//    void push_back(uint32_t t){
//        _data[_size++] = t;
//    }
//    int size(){ return _size; }
//    void clear(){ _size = 0; }
//    uint32_t* begin(){ return _data; }
//    uint32_t* end(){ return _data + _size; }
//    uint32_t& operator[](int index){ return _data[index]; }
//private:
//    uint32_t* _data;
//    int _size;
//};
//Vector ID; //　真实ID表
//
//// 简易的固定桶数hash_table，采用开放寻址法解决冲突
//class Hash_map{
//public:
//    Hash_map(){
//        map.resize(m);
//    }
//    void insert(uint32_t k){
//        // 如果不存在则插入，否则直接返回，作用等同：
//        // if(!Map.count(key)) Map.insert(key);
//        int i = 0;
//        int hash_val = 0;
//        while (i < m) {
//            hash_val = hash(k, i);
//            if (map[hash_val].val == -1){
//                map[hash_val].key = k;
//                map[hash_val].val = ID.size();
//                map[hash_val].len = 1;
//                ID.push_back(k);
//                hashes.push_back(hash_val);
//                cnt++;
//                break;
//            }
//            if(map[hash_val].key == k){
//                map[hash_val].len++;
//                break;
//            }else i++;
//        }
//    }
//    int search(uint32_t k){
//        // 搜索
//        int i = 0, hash_val = 0;
//        while (i < m) {
//            hash_val = hash(k, i);
//            if(map[hash_val].val == -1) break;
//            if(map[hash_val].key == k) return map[hash_val].val;
//            else i++;
//        }
//        return -1;
//    }
//    void sort_hash(){
//        // 将hash值排序，确保id映射前后相对大小不变化
//        sort(hashes.begin(), hashes.end(), [&](int a, int b){
//            return map[a].key < map[b].key;
//        });
//        for(int i = 0; i < hashes.size(); ++i){
//            map[hashes[i]].val = i;
//        }
//    }
//    int size(){
//        return cnt;
//    }
//private:
//    // 常数取质数，且 m1 < m
//    const int m = 1000039;
//    const int m1 = 1000037;
//    int cnt = 0;
//    // 数据结构
//    struct data{
//        uint32_t len = 0;
//        uint32_t key;
//        int val = -1;
//    };
//    vector<data> map;
//    vector<int> hashes;
//    uint32_t hash(uint32_t k, int i){
//        // 哈希函数
//        return k % m + i; // 一次哈希
//        // return (k % m + i * (m1 - k % m1)) % m; // 双重哈希
//    }
//}Map;account

const char* test_data_file = "/Users/milk/test_data/loop/scale1/account.csv";
//const char* result_file = "/projects/student/result.txt";
char* file;
int file_size;

void* load_thread(void* args){
    // 多线程读图
    int tid = *(int*)args;
    int size = file_size / nthread;
    if(tid == nthread - 1) size = file_size - (nthread - 1) * size;
    
    char* start = file + tid * (file_size / nthread);
    char* curr = start;

    // 确保两个线程不读到分割位置的同一行
    if(tid != 0 && *(curr - 1) != '\n') while(*curr++ != '\n');

    uint64_t temp = 0;
//    double money = 0;
    
    char ch;
//    int judge = 0;
    while(1){
        ch = *curr;
/*        if(ch == ','){
            loadbuf_edge[tid][loadnum[tid]][judge] = temp;
//            cout<<temp<<endl;
            judge++;
            temp = 0;
        }
        else if(ch == '.'){
            money = temp;
            temp = 0;
        }
        else */if(ch == '\r' || ch == '\n'){
//            judge = 0;
//            loadbuf_money[tid][loadnum[tid]] = money + (double)temp/100;
//            cout<<money + (double)temp/100<<endl;
            loadbuf_v[tid][loadnum[tid]] = temp;
//            cout<<temp<<endl;
            loadnum[tid]++;
            if(ch == '\r') curr++;
            if(curr - start + 1 >= size) break;
            temp = 0;
//            money = 0;
        }
        else{
            temp = temp * 10 + ch - '0';
        }
        curr++;
    }
//    return 0;
}

void load_data(){
/*
    @brief: load data from file "/data/test_data.txt"
    @method: mmap
*/
    // mmap
    struct stat statue;
    int fd = open(test_data_file, O_RDONLY);
    fstat(fd, &statue);
    file_size = statue.st_size;
    file = (char*)mmap(NULL, statue.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    // 1.多线程读数据到loadbuf中
    pthread_t threads[nthread];
    int tid[nthread];
    for(int i = 0; i < nthread; ++i){
        tid[i] = i;
        int result = pthread_create(&threads[i], NULL, load_thread, (void*)&tid[i]);
        if (result == 0) {
                cout<<"成功"<<endl;
            }
            else{
                cout<<"失败"<<endl;
            }
    }
    for(int i = 0; i < nthread; ++i)
        pthread_join(threads[i], NULL);

    // 2.哈希（仅映射 {u, v, w} 中的 u，因为不在 U 中出现的 id　肯定不成环）
//    for(int k = 0; k < nthread; ++k){
//        for(int i = 0; i < loadnum[k]; ++i){
//                Map.insert(loadbuf[k][i][0]);
//        }
//    }
//    // 3.哈希排序，确保映射前后相对大小不变
//    for(int i = 0; i < 2; ++i){
//        tid[i] = i;
//        pthread_create(&threads[i], NULL, shit_thread, (void*)&tid[i]);
//    }
//    for(int i = 0; i < 2; ++i)
//        pthread_join(threads[i], NULL);
//
//    // 4.将不用的记录删除(使得后续不用再查询哈系表)
//    for(int i = 0; i < nthread; ++i){
//        tid[i] = i;
//        pthread_create(&threads[i], NULL, clear_thread, (void*)&tid[i]);
//    }
//    for(int i = 0; i < nthread; ++i)
//        pthread_join(threads[i], NULL);
//
//    // 5.构造前向星
//    for(int i = 0; i < 2; ++i){
//        tid[i] = i;
//        pthread_create(&threads[i], NULL, build_thread, (void*)&tid[i]);
//    }
//    for(int i = 0; i < 2; ++i)
//        pthread_join(threads[i], NULL);
//
//    // ６.前向星排序
//    for(int i = 0; i < nthread; ++i){
//        tid[i] = i;
//        pthread_create(&threads[i], NULL, sort_thread, (void*)&tid[i]);
//    }
//    for(int i = 0; i < nthread; ++i)
//        pthread_join(threads[i], NULL);
}

int main(int argc, const char * argv[]) {
    // insert code here...
    std::cout << "Hello, World!\n";
    clock_t startTime,endTime;
    startTime = clock();//计时开始
    load_data();
    endTime = clock();//计时结束
    cout << "The read graph time is: " <<(double)(endTime - startTime) / CLOCKS_PER_SEC << "s" << endl;
    return 0;
}
