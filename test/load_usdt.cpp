#include <cstdio>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <omp.h>
#include "bind/livegraph.hpp"
#include "core/livegraph.hpp"
#include "core/transaction.hpp"
#include <fstream>
#include <sstream>
#include <ctime>
#include <queue>
#include <iostream>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sys/stat.h>

using namespace std;
using namespace livegraph;

label_t label = 1;
unordered_map<string,vertex_t> add2id;
unordered_map<vertex_t,string> id2add;
vertex_t max_vertex_id = 0;
int vertex_num = 0;
int edge_num = 0;

class query{
    public:
        vertex_t target;
        int k;
        query(vertex_t target_, int k_):
            target(target_), k(k_){};
        
};

class edge{
    private:
        string from_account;
        string to_account;
        int block_number;
    public:
        edge(string from_account_, string to_account_, int block_number_):
            from_account(from_account_), to_account(to_account_), block_number(block_number_){};
};

class query_result{
    private:
        int count;
        float elapsed_time;
    public:
        query_result(int count_, float elapsed_time_):
            count(count_), elapsed_time(elapsed_time_){};
        int getCount(){return count;};
        float getElapsedTime(){return elapsed_time;};
};

class bfs_result{
    private:
        float query_time;
        float resolve_time;
    public:
        bfs_result(float query_time_, float resolve_time_):
            query_time(query_time_), resolve_time(resolve_time_){};
        float getQueryTime(){return query_time;};
        float getResolveTime(){return resolve_time;};
};

int getFileLineCount(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        return -1; // Return -1 to indicate file open failure
    }

    int lineCount = 0;
    std::string line;
    while (std::getline(file, line)) {
        lineCount++;
    }

    file.close();
    return lineCount;
}

void updateProgressBar(int currentNum, int totalNum, int progressBarWidth) {
    float progressRatio = static_cast<float>(currentNum) / totalNum;
    int progressWidth = static_cast<int>(progressRatio * progressBarWidth);

    std::string progressBar;
    progressBar += "[";
    for (int i = 0; i < progressWidth; ++i) {
        progressBar += "=";
    }
    for (int i = progressWidth; i < progressBarWidth; ++i) {
        progressBar += " ";
    }
    progressBar += "] " + std::to_string(static_cast<int>(progressRatio * 100)) + "%";

    // Move the cursor to the beginning of the line and overwrite the previous progress bar
    std::cout << "\r" << progressBar << std::flush;
}

void load_vertex(string path, Graph& g) {
    auto start = std::chrono::high_resolution_clock::now();
    int line_count = getFileLineCount(path);
    std::cout << "loading "<< line_count << " vertex" << std::endl;
    // 开启事务
    // Transaction t = g.begin_transaction();
    Transaction t = g.begin_batch_loader();
    // std::vector<std::string> vertices;
    std::ifstream file(path);
    std::string line;
    int count = 0;
    while (std::getline(file, line)) {
        // vertices.push_back(line);
        vertex_t id = t.new_vertex();
        max_vertex_id = id;
        add2id[line] = id;
        id2add[id] = line;
        t.put_vertex(id, line);
        count += 1;
        if (count % 10000 == 0) {
            updateProgressBar(count, line_count, 80);
        }
    }
    updateProgressBar(count, line_count, 80);
    std::cout << std::endl;
    file.close();
    // 提交事务
    t.commit();
    // return vertices;
    // std::cout << "loading " << count << " vertices" << std::endl;
    vertex_num = count;
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "duration: " << duration.count() << " ms" << std::endl;
}

void load_edge(string path, Graph& g) {
    auto start = std::chrono::high_resolution_clock::now();

    int line_count = getFileLineCount(path);
    std::cout << "loading "<< line_count << " edge" << std::endl;
    // std::vector<edge> edges;
    std::ifstream file(path);
    std::string line;
    int count = 0;
    // 开启事务
    // Transaction t = g.begin_transaction();
    Transaction t = g.begin_batch_loader();
    while (std::getline(file, line)) { 
        std::istringstream ss(line);
        std::string from_account, to_account;
        int block_number;
        std::getline(ss, from_account, ',');
        std::getline(ss, to_account, ',');
        ss >> block_number;
        // 不带版本
        // t.put_edge(add2id[from_account], label, add2id[to_account], std::to_string(block_number));
        // 带版本
        t.put_edge_with_version(add2id[from_account], label, add2id[to_account], std::to_string(block_number), block_number);
        count += 1;
        if (count % 10000 == 0) {
            updateProgressBar(count, line_count, 80);
        }
    }
    updateProgressBar(count, line_count, 80);
    std::cout << std::endl;
    // std::cout << "ready to commit" << std::endl;
    timestamp_t commit_time = t.commit();
    // std::cout << "commit time: " << commit_time << std::endl;
    file.close();
    // std::cout << "loading " << count << " edges" << std::endl;
    edge_num = count;

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "duration: " << duration.count() << " ms" << std::endl;
}

void query1(Graph& g, vertex_t vertex_id) {
    // 开启事务
    std::cout << "start querying" << std::endl;
    Transaction t = g.begin_read_only_transaction();
    std::vector<std::string_view> edges = t.get_edge_with_version(0, label, 1, timestamp_t(1), timestamp_t(10));
    std::cout << "=====results=====" << std::endl;
    for (int i = 0; i < edges.size(); i++) {
        cout << edges[i] << endl;
    }
    // std::string_view ans = t.get_edge(0, label, 1);
    // cout << ans << endl;
}

void query2(Graph& g, vertex_t vertex_id) {
    // 开启事务
    std::cout << "start querying" << std::endl;
    Transaction t = g.begin_read_only_transaction();
    auto edge_iter=t.get_edges_with_version(vertex_id, label, timestamp_t(0), timestamp_t(10000000));
    while(edge_iter.valid())
    {
        vertex_t dst=edge_iter.dst_id();
        std::string_view data=edge_iter.edge_data();
        cout<<"dst: "<<dst<<" data: "<<data<<endl;
        edge_iter.next();
    }
    // std::string_view ans = t.get_edge(0, label, 1);
    // cout << ans << endl;
}


bfs_result k_hop_bfs(Transaction& t, int k, vertex_t target, timestamp_t start, timestamp_t end, int& count) {
    float query_time = 0;
    float resolve_time = 0;
    // std::cout << "processing " << k << "-hop query for " << id2add[target] << std::endl;
    std::queue<query> khop_queue;
    std::unordered_set<vertex_t> visit;

    khop_queue.push(query(target, k));
    visit.insert(target);
    while(!khop_queue.empty()){
        query q = khop_queue.front();
        khop_queue.pop();
        vertex_t tmp_target = q.target;
        int tmp_k = q.k;

        auto start_time = std::chrono::high_resolution_clock::now();
        // 记录查询时间
        auto edge_iter = t.get_edges_with_version(tmp_target, label, start, end);

        auto end_time_query = std::chrono::high_resolution_clock::now();
        std::chrono::duration<float, std::milli> duration_query = end_time_query - start_time;
        query_time += duration_query.count();


        int sub_count = 0;
        while(edge_iter.valid()) {
            vertex_t dst = edge_iter.dst_id();
            timestamp_t version = edge_iter.version();
            std::string_view data=edge_iter.edge_data();
            // cout<< id2add[target] << "," << id2add[dst] << "," << version << endl;
            // 这边的count += 1 的位置可能有点问题
            count += 1;
            sub_count += 1;
            auto ans = visit.find(dst);

            // 没检查过
            if (ans == visit.end()) {
                if (tmp_k > 1) {
                    // std::clock_t start_time = std::clock();

                    visit.insert(dst);
                    khop_queue.push(query(dst, tmp_k - 1));

                    // std::clock_t end_time = std::clock();
                    // double elapsed_time = static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC * 1000;
                    // std::cout << "part time:" << elapsed_time << std::endl;
                }
            }
            edge_iter.next();
        }
        auto end_time_resolve = std::chrono::high_resolution_clock::now();
        std::chrono::duration<float, std::milli> duration_resolve = end_time_resolve - end_time_query;
        resolve_time += duration_resolve.count();
        // std::cout << "1-hop for " << id2add[tmp_target] << ": " << sub_count << std::endl;
    }
    return bfs_result(query_time, resolve_time);
}

std::pair<query_result, bfs_result> k_hop_query(Graph& g, int k, vertex_t target, timestamp_t start, timestamp_t end) {
    int count = 0;


    auto start_time = std::chrono::high_resolution_clock::now();


    Transaction t = g.begin_read_only_transaction();

    bfs_result bfs_res = k_hop_bfs(t, k, target, start, end, count);

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float, std::milli> duration = end_time - start_time;
    float elapsed_time = duration.count();

    std::cout << "num of results: " << count << std::endl;
    std::cout << "elapsed time: " << elapsed_time << " ms" << std::endl;
    return {query_result(count, elapsed_time), bfs_res};
}

void count_size(Graph& g, vertex_t max_vertex_id) {
    Transaction t = g.begin_read_only_transaction();
    t.count_size(max_vertex_id);
}


void file_test(Graph& g, string input_path, string output_path, int k) {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::ifstream file(input_path);
    std::ofstream outFile(output_path);
    std::string line;
    outFile << "address,start_block,end_block,result_count,total_time,query_time,resolve_time" << endl;
    while (std::getline(file, line)) { 
        stringstream ss(line);;
        std::string item, target;
        timestamp_t start_version, end_version;
        getline(ss, item, ',');
        target = item;
        getline(ss, item, ',');
        start_version = timestamp_t(stoi(item));
        getline(ss, item, ',');
        end_version = timestamp_t(stoi(item));

        std::pair<query_result, bfs_result> res = k_hop_query(g, k, add2id[target], start_version, end_version);

        int count = res.first.getCount();
        float elapsed_time = res.first.getElapsedTime();
        outFile << target << "," << start_version << "," << end_version << "," << count << "," << elapsed_time  << "," << res.second.getQueryTime()  << "," << res.second.getResolveTime() << endl;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float, std::milli> duration = end_time - start_time;
    float elapsed_time = duration.count();
    std::cout << "the log has been written to "<< output_path << std::endl;
    std::cout << "time of executing the whole test file: " << elapsed_time << " ms" << std::endl;
    std::cout << "=================================================="<<std::endl;
    outFile.close();
    file.close();
}

bool fileExists(const std::string& filePath) {
    std::ifstream file(filePath);
    return file.good();
}

std::string extractFileName(const std::string& filePath) {
    size_t lastSlashPos = filePath.find_last_of('/');
    size_t dotPos = filePath.find_last_of('.');
    
    if (lastSlashPos == std::string::npos || dotPos == std::string::npos || lastSlashPos >= dotPos) {
        return "";
    }
    
    // return filePath.substr(lastSlashPos + 1, dotPos - lastSlashPos - 1);
    return filePath.substr(0, dotPos);
}

std::string getCurrentTimestampAsString() {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::time_t currentTime = std::chrono::system_clock::to_time_t(now);
    std::tm* timeInfo = std::localtime(&currentTime);

    std::ostringstream oss;
    oss << std::put_time(timeInfo, "%H%M%S");
    return oss.str();
}


int main(){
    Graph g = Graph("/home/lys/LiveGraph/block_path","/home/lys/LiveGraph/wal_path");
    std::string file_path = "/home/lys/LiveGraph/data/";
    std::string file_name = "usdt_1200_1700";
    // load_vertex("/home/lys/LiveGraph/data/usdt_1600_1700_vertex.txt", g);
    // load_edge("/home/lys/LiveGraph/data/usdt_1600_1700_edge.txt", g);
    load_vertex(file_path + file_name + "_vertex.txt", g);
    load_edge(file_path + file_name + "_edge.txt", g);

    while (true) {
        std::string input_file;
        std::cout << "Enter a file path: ";
        std::getline(std::cin, input_file);
        std::cout << input_file << std::endl;
        if (!fileExists(input_file)) {
            std::cout << "File does not exist." << std::endl;
        } else {
            int k;
            std::cout << "Enter an integer value for k: ";
            std::cin >> k;
            std::string file_name = extractFileName(input_file);
            std::string output_file = file_name + "_" + getCurrentTimestampAsString() + ".log";    
            file_test(g, input_file, output_file, k);
        }


        // 清除输入缓冲区中的换行符
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return 1;
}