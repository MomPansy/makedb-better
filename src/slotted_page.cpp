#include "slotted_page.h"

std::vector<ReturnType> SlottedPage::insert(std::vector<Data> serializedData, char *page, size_t pageSize, PageDirectoryEntry &entry)
{
    // 1. Check overall page validity
    logger_.log("Checking overall page validity");
    if (!page || pageSize != PAGE_SIZE)
    {
        throw std::runtime_error("Invalid page page: must be PAGE_SIZE bytes.");
    }

    // 2. Read the header
    logger_.log("Reading the header");
    SlottedPageHeader localHeader;
    std::memcpy(&localHeader, page, sizeof(SlottedPageHeader));

    // 3. Sanity checks on header
    //    a) Ensure numSlots is not too large.
    //       For instance, we can require that the slot directory
    //       fits between the header and freeDataOffset.
    logger_.log("Sanity checks on header");
    const uint16_t maxSlots = (PAGE_SIZE - sizeof(SlottedPageHeader)) / sizeof(SlotEntry);
    if (localHeader.numSlots > maxSlots)
    {
        throw std::runtime_error(
            "Corrupt page header: numSlots exceeds possible slot directory capacity.");
    }

    //    b) Ensure freeDataOffset is at most PAGE_SIZE (cannot point outside the page).
    if (localHeader.freeDataOffset > PAGE_SIZE)
    {
        logger_.log("Corrupt page header: freeDataOffset is beyond the page size");
        throw std::runtime_error(
            "Corrupt page header: freeDataOffset is beyond the page size.");
    }

    //    c) Ensure slot directory does not overlap free space region.
    //       The slot directory area extends from sizeof(SlottedPageHeader) to
    //       sizeof(SlottedPageHeader) + numSlots * sizeof(SlotEntry).
    //       That must be less than or equal to freeDataOffset (which grows downward).
    const size_t slotDirectoryEnd = sizeof(SlottedPageHeader) +
                                    localHeader.numSlots * sizeof(SlotEntry);

    logger_.log("Checking slot directory does not overlap free space region");
    if (slotDirectoryEnd > localHeader.freeDataOffset)
    {
        throw std::runtime_error(
            "Corrupt page header: slot directory overlaps data region.");
    }

    // At this point, the header looks valid for further insert logic...
    // Insert code continues below:

    std::vector<ReturnType> results;
    results.reserve(serializedData.size());

    for (auto &d : serializedData)
    {
        size_t rowSize = d.data.size();

        // offset for the next slot entry
        size_t slotDirOffset = sizeof(SlottedPageHeader) + localHeader.numSlots * sizeof(SlotEntry);

        // location for the row’s payload
        size_t dataOffset = localHeader.freeDataOffset - rowSize;

        // Check if there is enough space
        if (dataOffset < slotDirOffset)
        {
            throw std::runtime_error("Not enough space for new row in slotted page.");
        }

        // Copy row data into page
        std::memcpy(page + dataOffset, d.data.data(), rowSize);

        // Create a new slot entry
        SlotEntry newSlot;
        newSlot.offset = static_cast<uint16_t>(dataOffset);
        newSlot.length = static_cast<uint16_t>(rowSize);

        // Write slot entry to slot directory area
        std::memcpy(page + slotDirOffset, &newSlot, sizeof(SlotEntry));

        // Update the header
        localHeader.numSlots++;
        localHeader.freeDataOffset = static_cast<uint16_t>(dataOffset);

        // Create return info
        ReturnType ret;
        ret.data.id = d.id;
        ret.location.page_id = entry.page_id; // or real page_id
        ret.location.slot_id = static_cast<uint16_t>(localHeader.numSlots - 1);

        results.push_back(ret);
    }

    // Write updated header back
    std::memcpy(page, &localHeader, sizeof(SlottedPageHeader));

    return results;
}