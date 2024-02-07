#include <vector>
#include <string>
#include <memory> // For smart pointers

// Forward declaration of the Table class
class Table;

class Database {
private:
    // Using smart pointers for automatic memory management
    std::vector<std::unique_ptr<Table>> tables;

public:
    Database() = default; // Default constructor

    // Not required for milestone1
    void open(const std::string& path) {
        // Implementation goes here
    }

    void close() {
        // Implementation goes here
    }

    /**
     * Creates a new table
     * @param name: string         // Table name
     * @param num_columns: int     // Number of Columns: all columns are integer
     * @param key_index: int       // Index of table key in columns
     */
    Table* create_table(const std::string& name, int num_columns, int key_index) {
        // Assuming Table constructor matches this signature
        auto table = std::make_unique<Table>(name, num_columns, key_index);
        Table* tablePtr = table.get();
        tables.push_back(std::move(table));
        return tablePtr;
    }

    /**
     * Deletes the specified table
     */
    void drop_table(const std::string& name) {
        // Implementation goes here
    }

    /**
     * Returns table with the passed name
     */
    Table* get_table(const std::string& name) {
        for (auto& table : tables) {
            if (table->getName() == name) { // Assuming Table has a getName() method
                return table.get();
            }
        }
        return nullptr; // If no table found
    }
};
