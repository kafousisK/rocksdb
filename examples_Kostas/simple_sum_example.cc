#include <cstdio>
#include <string>
#include <iostream>
#include <deque>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_sum_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_sum_example";
#endif

class SumMergeOperator : public rocksdb::MergeOperator {
public:
    bool FullMerge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
                   const std::deque<std::string>& operand_list, std::string* new_value,
                   rocksdb::Logger* logger) const override {
        int sum = 0;

        // If there is an existing value, start with its integer representation
        if (existing_value) {
            sum = std::stoi(existing_value->ToString());
        }

        // Iterate over each operand and add its integer representation to the sum
        for (const auto& operand : operand_list) {
            sum += std::stoi(operand);
        }

        // Store the sum as a string in the new value
        *new_value = std::to_string(sum);

        return true;
    }

    const char* Name() const override {
        return "SumMergeOperator";
    }
};

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    std::string value;
    // Set the merge operator to SumMergeOperator
    options.merge_operator.reset(new SumMergeOperator());
    options.create_if_missing=true;

    // Open the database
    rocksdb::Status status = rocksdb::DB::Open(options, kDBPath, &db);
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }

    // Perform a merge operation
    std::string key = "my_key";
    db->Merge(rocksdb::WriteOptions(), key, "5");
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (status.ok()) {
        std::cout << "Merged value for key " << key << ": " << value << std::endl;
    } else {
        std::cerr << "Failed to retrieve merged value: " << status.ToString() << std::endl;
    }
    db->Merge(rocksdb::WriteOptions(), key, "10");
    db->Merge(rocksdb::WriteOptions(), key, "15");
    //put must override old value of key 
    db->Put(rocksdb::WriteOptions(),key,"0");
    db->Merge(rocksdb::WriteOptions(), key, "20");
    db->Merge(rocksdb::WriteOptions(), key, "30");
    db->Merge(rocksdb::WriteOptions(), key, "7");
    db->Merge(rocksdb::WriteOptions(), key, "8");
    db->Merge(rocksdb::WriteOptions(), key, "5");

    

    // Retrieve the merged value

    status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (status.ok()) {
        std::cout << "Merged value for key " << key << ": " << value << std::endl;
    } else {
        std::cerr << "Failed to retrieve merged value: " << status.ToString() << std::endl;
    }

    // Close the database
    delete db;
    
    // code to destroy the database
    /*
    status = rocksdb::DestroyDB(kDBPath, options);
    if (!status.ok()) {
        std::cerr << "Failed to destroy database: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Database destroyed successfully." << std::endl;
    */
    return 0;
}