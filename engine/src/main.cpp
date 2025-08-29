#include "database.h"
#include <iostream>

int main() {
    std::cout << "Simple Database Engine - Full Demo" << std::endl;
    
    Database db("demo.db");
    
    std::cout << "\n=== Creating Table ===" << std::endl;
    db.ExecuteQuery("CREATE TABLE users (id INT, name VARCHAR, age INT)");
    
    std::cout << "\n=== Inserting Data ===" << std::endl;
    db.ExecuteQuery("INSERT INTO users VALUES (1, 'Alice', 25)");
    db.ExecuteQuery("INSERT INTO users VALUES (2, 'Bob', 30)");
    db.ExecuteQuery("INSERT INTO users VALUES (3, 'Charlie', 35)");
    
    std::cout << "\n=== Selecting All Records ===" << std::endl;
    if (db.ExecuteQuery("SELECT * FROM users")) {
        auto results = db.GetLastResults();
        std::cout << "Found " << results.size() << " records:" << std::endl;
        for (const auto& record : results) {
            std::cout << "  " << record.ToString() << std::endl;
        }
    }
    
    std::cout << "\n=== Selecting with WHERE Clause ===" << std::endl;
    if (db.ExecuteQuery("SELECT * FROM users WHERE id = 2")) {
        auto results = db.GetLastResults();
        std::cout << "Found " << results.size() << " records:" << std::endl;
        for (const auto& record : results) {
            std::cout << "  " << record.ToString() << std::endl;
        }
    }
    
    std::cout << "\n=== Database Demo Complete ===" << std::endl;
    return 0;
}