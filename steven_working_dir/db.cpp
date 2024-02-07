#ifndef DATABASE_H
#define DATABASE_H

#include <vector>
#include <string>
#include <memory>
// Include the Table class header
#include "table.hpp"   // Forward declaration to resolve circular dependency
#include "../../msys64/ucrt64/include/c++/13.1.0/bits/algorithmfwd.h"

class Database {
private:
    std::vector<std::unique_ptr<Table>> tables; // Using smart pointers for automatic memory management

public:
    // Constructor
    Database() {}

    // Open database from path - Not required for the milestone, but placeholder provided
    void open(const std::string& path) {
        // Implementation would go here
    }

    // Close the database
    void close() {
        // Cleanup or final actions before closing the database
        // Since we're using smart pointers, explicit deletion is not necessary
    }

    // Creates a new table
    Table* create_table(const std::string& name, int num_columns, int key_index) {
        // Using smart pointers to manage Table objects
        std::unique_ptr<Table> table = std::make_unique<Table>(name, num_columns, key_index);
        tables.push_back(std::move(table));
        return tables.back().get(); // Return a raw pointer to the newly created Table
    }

    // Deletes the specified table
    void drop_table(const std::string& name) {
        // Find and remove the table from the vector
        tables.erase(std::remove_if(tables.begin(), tables.end(),
                                    [&name](const std::unique_ptr<Table>& table) { return table->getName() == name; }),
                     tables.end());
    }

    // Returns table with the passed name
    Table* get_table(const std::string& name) {
        for (auto& table : tables) {
            if (table->getName() == name) {
                return table.get(); // Return a raw pointer to the Table
            }
        }
        return nullptr; // If no table with the name is found
    }
};

#endif // DATABASE_H
