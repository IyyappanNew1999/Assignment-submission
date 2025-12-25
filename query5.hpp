#pragma once
#include <string>
#include <vector>
#include <map>

struct Customer { int c_custkey; int c_nationkey; };
struct Orders { int o_orderkey; int o_custkey; std::string o_orderdate; };
struct Lineitem { int l_orderkey; int l_suppkey; double l_extendedprice; double l_discount; };
struct Supplier { int s_suppkey; int s_nationkey; };
struct Nation { int n_nationkey; std::string n_name; int n_regionkey; };
struct Region { int r_regionkey; std::string r_name; };

// ---- Function declarations ----
bool parseArgs(int argc, char* argv[],
               std::string& r_name,
               std::string& start_date,
               std::string& end_date,
               int& num_threads,
               std::string& table_path,
               std::string& result_path);

bool readTPCHData(const std::string& table_path,
                  std::vector<Customer>& customers,
                  std::vector<Orders>& orders,
                  std::vector<Lineitem>& lineitems,
                  std::vector<Supplier>& suppliers,
                  std::vector<Nation>& nations,
                  std::vector<Region>& regions);

bool executeQuery5(const std::string& r_name,
                   const std::string& start_date,
                   const std::string& end_date,
                   int num_threads,
                   const std::vector<Customer>& customers,
                   const std::vector<Orders>& orders,
                   const std::vector<Lineitem>& lineitems,
                   const std::vector<Supplier>& suppliers,
                   const std::vector<Nation>& nations,
                   const std::vector<Region>& regions,
                   std::map<std::string,double>& results);

bool outputResults(const std::string& result_path,
                   const std::map<std::string,double>& results);