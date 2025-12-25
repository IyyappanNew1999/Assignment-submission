#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <map>
#include <vector>
#include <set>
#include <algorithm>
#include <mutex>

// Parse command line arguments
bool parseArgs(int argc, char* argv[],
               std::string& r_name, std::string& start_date, std::string& end_date,
               int& num_threads, std::string& table_path, std::string& result_path) {
    if (argc != 7) {
        std::cerr << "Usage: " << argv[0]
                  << " <region> <start_date> <end_date> <threads> <table_path> <result_path>\n";
        return false;
    }

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        }
    }
    std::cout << "Region Name : " << r_name << "\n";
    std::cout << "Start Date  : " << start_date << "\n";
    std::cout << "End Date    : " << end_date << "\n";
    std::cout << "Threads     : " << num_threads << "\n";
    std::cout << "Table Path  : " << table_path << "\n";
    std::cout << "Result Path : " << result_path << "\n";

    return true;
}

// Split string
static inline std::vector<std::string> split(const std::string &line, char delim='|') {
    std::vector<std::string> tokens;
    std::stringstream ss(line);
    std::string item;
    while (std::getline(ss, item, delim))
        tokens.push_back(item);
    return tokens;
}

// Read data from .tbl files
bool readTPCHData(const std::string& table_path,
                  std::vector<Customer>& customers,
                  std::vector<Orders>& orders,
                  std::vector<Lineitem>& lineitems,
                  std::vector<Supplier>& suppliers,
                  std::vector<Nation>& nations,
                  std::vector<Region>& regions) {
    auto readFile = [&](const std::string& fname, auto& vec, auto parseLine) -> bool {
        std::ifstream file(table_path + "/" + fname);
        if (!file) {
            std::cerr << "Failed to open file: " << fname << "\n";
            return false;
        }
        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty()) vec.push_back(parseLine(split(line)));
        }
        return true;
    };

    return
        readFile("customer.tbl", customers,
                 [](const auto& f){ return Customer{std::stoi(f[0]), std::stoi(f[3])}; }) &&
        readFile("orders.tbl", orders,
                 [](const auto& f){ return Orders{std::stoi(f[0]), std::stoi(f[1]), f[4]}; }) &&
        readFile("lineitem.tbl", lineitems,
                 [](const auto& f){ return Lineitem{std::stoi(f[0]), std::stoi(f[2]),
                                                    std::stod(f[5]), std::stod(f[6])}; }) &&
        readFile("supplier.tbl", suppliers,
                 [](const auto& f){ return Supplier{std::stoi(f[0]), std::stoi(f[3])}; }) &&
        readFile("nation.tbl", nations,
                 [](const auto& f){ return Nation{std::stoi(f[0]), f[1], std::stoi(f[2])}; }) &&
        readFile("region.tbl", regions,
                 [](const auto& f){ return Region{std::stoi(f[0]), f[1]}; });
}

// Execute Query 5
bool executeQuery5(const std::string& r_name,
                   const std::string& start_date, const std::string& end_date,
                   int num_threads,
                   const std::vector<Customer>& customers,
                   const std::vector<Orders>& orders,
                   const std::vector<Lineitem>& lineitems,
                   const std::vector<Supplier>& suppliers,
                   const std::vector<Nation>& nations,
                   const std::vector<Region>& regions,
                   std::map<std::string,double>& results) {

    int target_region_key = -1;
    for (auto& r : regions) {
        if (r.r_name == r_name) {
            target_region_key = r.r_regionkey;
            break;
        }
    }
    if (target_region_key < 0) {
        std::cerr << "Region not found: " << r_name << "\n";
        return false;
    }

    std::set<int> nation_keys;
    std::map<int,std::string> nation_name;
    for (auto& n : nations) {
        if (n.n_regionkey == target_region_key) {
            nation_keys.insert(n.n_nationkey);
            nation_name[n.n_nationkey] = n.n_name;
        }
    }

    std::map<int,int> supp_to_nation;
    for (auto& s : suppliers) {
        if (nation_keys.count(s.s_nationkey))
            supp_to_nation[s.s_suppkey] = s.s_nationkey;
    }

    std::map<int,int> cust_to_nation;
    for (auto& c : customers) {
        if (nation_keys.count(c.c_nationkey))
            cust_to_nation[c.c_custkey] = c.c_nationkey;
    }

    std::mutex mtx;
    auto worker = [&](int start, int end) {
        std::map<std::string,double> local_results;
        for (int i = start; i < end; i++) {
            const auto& o = orders[i];
            if (o.o_orderdate >= start_date && o.o_orderdate < end_date) {
                auto cit = cust_to_nation.find(o.o_custkey);
                if (cit != cust_to_nation.end()) {
                    int cust_nation = cit->second;
                    for (const auto& li : lineitems) {
                        if (li.l_orderkey == o.o_orderkey) {
                            auto sit = supp_to_nation.find(li.l_suppkey);
                            if (sit != supp_to_nation.end() && sit->second == cust_nation) {
                                double revenue = li.l_extendedprice * (1.0 - li.l_discount);
                                local_results[nation_name[cust_nation]] += revenue;
                            }
                        }
                    }
                }
            }
        }
        std::lock_guard<std::mutex> lock(mtx);
        for (auto& kv : local_results)
            results[kv.first] += kv.second;
    };

    std::vector<std::thread> threads;
    int chunk = orders.size() / num_threads;
    for (int t = 0; t < num_threads; t++) {
        int start = t * chunk;
        int end = (t == num_threads - 1) ? orders.size() : (t + 1) * chunk;
        threads.emplace_back(worker, start, end);
    }
    for (auto& th : threads) th.join();

    return true;
}

// Output results
bool outputResults(const std::string& result_path, const std::map<std::string,double>& results) {
    std::vector<std::pair<std::string,double>> sorted(results.begin(), results.end());
    std::sort(sorted.begin(), sorted.end(),
              [](auto& a, auto& b){ return b.second < a.second; });
    std::ofstream out(result_path);
    if (!out) {
        std::cerr << "Failed to open output file: " << result_path << "\n";
        return false;
    }
    for (auto& [nation, revenue] : sorted)
        out << nation << " | " << revenue << "\n";
    return true;
}