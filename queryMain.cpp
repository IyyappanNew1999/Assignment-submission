#include "query5.hpp"
#include <iostream>

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads = 1;

    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        return 1;
    }

    std::vector<Customer> customers;
    std::vector<Orders> orders;
    std::vector<Lineitem> lineitems;
    std::vector<Supplier> suppliers;
    std::vector<Nation> nations;
    std::vector<Region> regions;

    if (!readTPCHData(table_path, customers, orders, lineitems, suppliers, nations, regions)) {
        std::cerr << "Error: Could not read TPCH data from " << table_path << "\n";
        return 1;
    }

    std::map<std::string,double> results;
    if (!executeQuery5(r_name, start_date, end_date, num_threads,
                       customers, orders, lineitems, suppliers, nations, regions, results)) {
        std::cerr << "Error: Failed to execute Query 5.\n";
        return 1;
    }

    if (!outputResults(result_path, results)) {
        std::cerr << "Error: Could not write results to " << result_path << "\n";
        return 1;
    }

    std::cout << "Query 5 executed successfully! Output saved to " << result_path << "\n";
    return 0;
}


/*