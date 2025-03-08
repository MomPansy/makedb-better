#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <filesystem>
#include <sstream>

#include "file_storage.h"
namespace fs = std::filesystem;

void ensureDirectoryExists(const std::string &filepath)
{
    fs::path p(filepath);
    fs::path directory = p.parent_path();
    if (!directory.empty() && !fs::exists(directory))
    {
        if (!fs::create_directories(directory))
        {
            throw std::runtime_error("Failed to create directory: " + directory.string());
        }
    }
}

bool FileStorage::writeFile(const std::string &filename, char *data, std::size_t size, std::streampos offset)
{
    ensureDirectoryExists(filename);
    std::ostringstream message;
    message << "Writing " << size << " bytes to file: " << filename;
    logger_.log(message.str());

    std::ofstream outFile(filename, std::ios::binary | std::ios::out);
    if (!outFile)
    {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    outFile.seekp(offset);
    outFile.write(data, size);
    outFile.close();

    return true;
}

bool FileStorage::readFile(const std::string &filename, char *buffer, std::size_t size, std::streampos offset)
{
    ensureDirectoryExists(filename);
    std::ostringstream message;
    message << "Attempting to open file: " << filename << " of size: " << size << " bytes";
    logger_.log(message.str());
    message.str("");

    std::ifstream inFile(filename, std::ios::binary | std::ios::in);
    if (!inFile)
    {
        throw std::runtime_error("Failed to open file for reading: " + filename);
    }
    inFile.seekg(0, std::ios::end);
    std::streampos fileSize = inFile.tellg();

    message << "File size: " << fileSize << " bytes";
    logger_.log(message.str());
    message.str("");

    if (offset >= fileSize)
    {
        throw std::runtime_error("Offset is greater than file size: " + filename);
    }

    message << "Seeking to offset: " << offset << "in file: " << filename;
    logger_.log(message.str());
    message.str("");

    inFile.seekg(offset, std::ios::beg);
    if (!inFile)
    {
        throw std::runtime_error("Failed to seek to offset: " + std::to_string(offset) + " in file: " + filename);
    }

    // pre-zero the buffer
    std::fill(buffer, buffer + size, 0);
    message << "Reading " << size << " bytes from file: " << filename;
    logger_.log(message.str());
    message.str("");

    inFile.read(buffer, size);
    std::streamsize bytesRead = inFile.gcount();

    if (bytesRead < static_cast<std::streamsize>(size))
    {
        message << "Partial Read " << bytesRead << " bytes from file: " << filename << ". Expected " << size << " bytes.";
        throw std::runtime_error(message.str());
    }
    else if (bytesRead == static_cast<std::streamsize>(size))
    {
        message << "Successfully read " << bytesRead << " bytes from file: " << filename;
    }
    else
    {
        message << "Error reading file: " << filename;
        throw std::runtime_error(message.str());
    }
    logger_.log(message.str());
    inFile.close();
    return true;
}

void FileStorage::appendFile(const std::string &filename, const char *data, std::size_t size)
{
    ensureDirectoryExists(filename);
    std::ostringstream message;
    message << "Appending " << size << " bytes to file: " << filename;
    logger_.log(message.str());

    std::ofstream outFile(filename, std::ios::binary | std::ios::app);
    if (!outFile)
    {
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

bool FileStorage::createFile(const std::string &filename)
{
    std::ofstream file(filename);
    if (!file)
    {
        throw std::runtime_error("Failed to create file: " + filename);
    }
    return true;
}

size_t FileStorage::getSize(const std::string &filename)
{
    logger_.log("Checking file size for: " + filename);

    // If the file doesn't exist, either return 0 or throw an error.
    if (!fileExists(filename))
    {
        logger_.log("File does not exist: " + filename);
        return 0; 
        // or throw std::runtime_error("File not found: " + filename);
    }

    // Open file in binary mode, positioned at the end (ate).
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file)
    {
        throw std::runtime_error("Failed to open file for size check: " + filename);
    }

    // tellg() gives us the number of bytes from the beginning to current position (end).
    std::streampos fileSize = file.tellg();
    file.close();

    if (fileSize < 0)
    {
        throw std::runtime_error("tellg() returned negative for file: " + filename);
    }

    // Log and return the size as a size_t.
    logger_.log("File size is " + std::to_string(fileSize) + " bytes for: " + filename);
    return static_cast<size_t>(fileSize);
}
