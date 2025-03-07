#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <filesystem>

#include "file_storage.h"
namespace fs = std::filesystem;

void ensureDirectoryExists(const std::string &filepath)
{
    fs::path p(filepath);
    fs::path directory = p.parent_path();
    if (!directory.empty() && !fs::exists(directory)) {
        if (!fs::create_directories(directory)) {
            throw std::runtime_error("Failed to create directory: " + directory.string());
        }
    }
} 

void FileStorage::writeFile(const std::string &filename, char *data, std::size_t size, std::streampos offset)
{
    ensureDirectoryExists(filename); 
    std::ostringstream message;
    message << "Writing " << size << " bytes to file: " << filename;
    logger_.log(message.str());
    
    std::ofstream outFile(filename, std::ios::binary | std::ios::out);
    if (!outFile) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    outFile.seekp(offset);
    outFile.write(data, size);
    outFile.close();
} 

void FileStorage::readFile(const std::string &filename, char *buffer, std::size_t size, std::streampos offset)
{
    ensureDirectoryExists(filename);
    std::ostringstream message;
    message << "Attempting to open file: " << filename << " of size: " << size << " bytes";
    logger_.log(message.str());
    message.str("");
    
    std::ifstream inFile(filename, std::ios::binary | std::ios::in);
    if (!inFile) {
        throw std::runtime_error("Failed to open file for reading: " + filename);
    }
    inFile.seekg(0, std::ios::end); 
    std::streampos fileSize = inFile.tellg(); 
    
    message << "File size: " << fileSize << " bytes";
    logger_.log(message.str());
    message.str("");

    if (offset >= fileSize) {
        throw std::runtime_error("Offset is greater than file size: " + filename);
    }

    message << "Seeking to offset: " << offset << "in file: " << filename;
    logger_.log(message.str());
    message.str("");

    inFile.seekg(offset, std::ios::beg); 
    if(!inFile) {
        throw std::runtime_error("Failed to seek to offset: " + std::to_string(offset) + " in file: " + filename);
    }

    // pre-zero the buffer 
    std::fill(buffer, buffer + size, 0);
    message << "Reading " << size << " bytes from file: " << filename;
    logger_.log(message.str());
    message.str("");

    inFile.read(buffer, size);
    std::streamsize bytesRead = inFile.gcount();

    if (bytesRead < static_cast<std::streamsize>(size)) {
        message << "Partial Read " << bytesRead << " bytes from file: " << filename << ". Expected " << size << " bytes.";
    } else if (bytesRead == static_cast<std::streamsize>(size)) {
        message << "Successfully read " << bytesRead << " bytes from file: " << filename;
    } else {
        message << "Error reading file: " << filename;
    }
    logger_.log(message.str());
    inFile.close();
}

void FileStorage::appendFile(const std::string &filename, const char *data, std::size_t size)
{
    ensureDirectoryExists(filename); 
    std::ostringstream message;
    message << "Appending " << size << " bytes to file: " << filename;
    logger_.log(message.str());
    
    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    if (!outFile) {
        throw std::runtime_error("Failed to open file for appending: " + filename);
    }
    outFile.write(data, size);
    outFile.close();
}

bool FileStorage::fileExists(const std::string &filename)
{
    std::ifstream file(filename);
    return file.good();
}