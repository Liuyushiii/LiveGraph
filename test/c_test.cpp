#include <cstdio>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <omp.h>
#include <iostream>
#include "bind/livegraph.hpp"
#include "core/livegraph.hpp"
#include "core/transaction.hpp"

using namespace std;
using namespace livegraph;

//映射地址和节点下标
unordered_map<string,vertex_t> ad_id;
unordered_map<vertex_t,string> id_ad;

int main()
{
    for (int i = 0; i < 30; i++) {
        for (int j = 1; j <= 3; j++) {
            string file_name = "/home/lys/LiveGraph/data/test_case/" + to_string(j) + "-hop_result_" + to_string(i+1) + ".txt";
            cout << file_name << endl;
        }
    }
}