    #include "query5.hpp"
    #include <iostream>
    #include <fstream>
    #include <sstream>
    #include <thread>
    #include <mutex>
    #include <algorithm>

    // Function to parse command line arguments
    bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
        // TODO: Implement command line argument parsing
        // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--r_name" && i + 1 < argc) r_name = argv[++i];
            else if (arg == "--start_date" && i + 1 < argc) start_date = argv[++i];
            else if (arg == "--end_date" && i + 1 < argc) end_date = argv[++i];
            else if (arg == "--threads" && i + 1 < argc) num_threads = std::stoi(argv[++i]);
            else if (arg == "--table_path" && i + 1 < argc) table_path = argv[++i];
            else if (arg == "--result_path" && i + 1 < argc) result_path = argv[++i];
            else return false;
        }
        return true;
    }   

    bool loadTable(const std::string& filepath, const std::vector<std::string>& columns, std::vector<std::map<std::string, std::string>>& data) {
        std::ifstream file(filepath);
        if (!file.is_open()) return false;

        std::string line;
        while (std::getline(file, line)) {
            std::map<std::string, std::string> row;
            std::stringstream ss(line);
            std::string token;
            size_t i = 0;
            while (std::getline(ss, token, '|') && i < columns.size()) {
                row[columns[i]] = token;
                ++i;
            }
            data.push_back(row);
        }
        return true;
    }


    // Function to read TPCH data from the specified paths
    bool readTPCHData(const std::string& path,
        std::vector<std::map<std::string, std::string>>& customer_data,
        std::vector<std::map<std::string, std::string>>& orders_data,
        std::vector<std::map<std::string, std::string>>& lineitem_data,
        std::vector<std::map<std::string, std::string>>& supplier_data,
        std::vector<std::map<std::string, std::string>>& nation_data,
        std::vector<std::map<std::string, std::string>>& region_data) {
        return loadTable(path + "/customer.tbl", { "C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY" }, customer_data) &&
            loadTable(path + "/orders.tbl", { "O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE" }, orders_data) &&
            loadTable(path + "/lineitem.tbl", { "L_ORDERKEY", "L_SUPPKEY", "L_EXTENDEDPRICE", "L_DISCOUNT" }, lineitem_data) &&
            loadTable(path + "/supplier.tbl", { "S_SUPPKEY", "S_NATIONKEY" }, supplier_data) &&
            loadTable(path + "/nation.tbl", { "N_NATIONKEY", "N_NAME", "N_REGIONKEY" }, nation_data) &&
            loadTable(path + "/region.tbl", { "R_REGIONKEY", "R_NAME" }, region_data);
    }

    // Function to execute TPCH Query 5 using multithreading
    bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads,
        const std::vector<std::map<std::string, std::string>>& customer_data,
        const std::vector<std::map<std::string, std::string>>& orders_data,
        const std::vector<std::map<std::string, std::string>>& lineitem_data,
        const std::vector<std::map<std::string, std::string>>& supplier_data,
        const std::vector<std::map<std::string, std::string>>& nation_data,
        const std::vector<std::map<std::string, std::string>>& region_data,
        std::map<std::string, double>& results) {

        std::mutex result_mutex;

        // 1. Filter region
        std::vector<std::string> region_keys;
        for (const auto& row : region_data) {
            if (row.at("R_NAME") == r_name) region_keys.push_back(row.at("R_REGIONKEY"));
        }

        // 2. Nation -> Region join
        std::map<std::string, std::string> nation_to_name;
        std::vector<std::string> nation_keys;
        for (const auto& row : nation_data) {
            if (std::find(region_keys.begin(), region_keys.end(), row.at("N_REGIONKEY")) != region_keys.end()) {
                nation_keys.push_back(row.at("N_NATIONKEY"));
                nation_to_name[row.at("N_NATIONKEY")] = row.at("N_NAME");
            }
        }

        // 3. Supplier -> Nation join
        std::vector<std::string> supplier_keys;
        std::map<std::string, std::string> supp_to_nation;
        for (const auto& row : supplier_data) {
            if (std::find(nation_keys.begin(), nation_keys.end(), row.at("S_NATIONKEY")) != nation_keys.end()) {
                supplier_keys.push_back(row.at("S_SUPPKEY"));
                supp_to_nation[row.at("S_SUPPKEY")] = row.at("S_NATIONKEY");
            }
        }

        // Thread function
        auto worker = [&](int start, int end) {
            std::map<std::string, double> local_result;
            for (int i = start; i < end; ++i) {
                const auto& li = lineitem_data[i];
                std::string supp = li.at("L_SUPPKEY");
                if (supp_to_nation.find(supp) == supp_to_nation.end()) continue;

                std::string orderkey = li.at("L_ORDERKEY");

                // Find matching order
                auto order_it = std::find_if(orders_data.begin(), orders_data.end(), [&](const auto& o) {
                    return o.at("O_ORDERKEY") == orderkey && o.at("O_ORDERDATE") >= start_date && o.at("O_ORDERDATE") < end_date;
                    });

                if (order_it == orders_data.end()) continue;

                // Find matching customer
                auto cust_it = std::find_if(customer_data.begin(), customer_data.end(), [&](const auto& c) {
                    return c.at("C_CUSTKEY") == order_it->at("O_CUSTKEY") &&
                        c.at("C_NATIONKEY") == supp_to_nation[supp];
                    });

                if (cust_it == customer_data.end()) continue;

                std::string nation_name = nation_to_name[supp_to_nation[supp]];
                double ext_price = std::stod(li.at("L_EXTENDEDPRICE"));
                double discount = std::stod(li.at("L_DISCOUNT"));
                local_result[nation_name] += ext_price * (1.0 - discount);
            }

            std::lock_guard<std::mutex> lock(result_mutex);
            for (const auto& [nation, revenue] : local_result) {
                results[nation] += revenue;
            }
            };

        // Launch threads
        std::vector<std::thread> threads;
        int chunk = lineitem_data.size() / num_threads;
        for (int i = 0; i < num_threads; ++i) {
            int start = i * chunk;
            int end = (i == num_threads - 1) ? lineitem_data.size() : start + chunk;
            threads.emplace_back(worker, start, end);
        }
        for (auto& t : threads) t.join();
        return true;
    }

    // Function to output results to the specified path
    bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
        // TODO: Implement outputting results to a file
        std::ofstream out(result_path);
        if (!out.is_open()) return false;
        for (const auto& [nation, revenue] : results) {
            out << nation << "|" << revenue << "\n";
        }
        return true;
    } 