#pragma once
#include <cstddef>
#include <ios> 

class IStorage {
    public:
        virtual ~IStorage() = default; 
        virtual void writeFile(const std::string &filename, char *data, std::size_t size, std::streampos offset = 0) = 0; 
        virtual void readFile(const std::string &filename, char *data, std::size_t size, std::streampos offset = 0) = 0;
        virtual void appendFile(const std::string &filename, const char *data, std::size_t size) = 0;
        virtual bool fileExists(const std::string &filename) = 0;
};