/// lineage_buffer.cpp
/// Implements lineage buffer data structures and logging utilities.

#include "lineage_extension.hpp"
#include "lineage/lineage_init.hpp"

#include <iostream>

namespace duckdb {

/// Print an IVector of simple scalar types (int, bigint, etc.)
void PrintLoggedVector(const IVector &entry, idx_t type_size) {
    std::cout << "IVector count: " << entry.count << "\n";
    // --- Print selection vector ---
    if (entry.sel) {
        std::cout << "Selection vector: ";
        for (idx_t i = 0; i < entry.count; i++) {
            std::cout << entry.sel[i] << " ";
        }
        std::cout << "\n";
    } else {
        std::cout << "Selection vector: nullptr (flat)\n";
    }

    // --- Print data buffer ---
    std::cout << "Data buffer: ";
    for (idx_t i = 0; i < entry.count; i++) {
        idx_t idx = entry.sel ? entry.sel[i] : i;

        // print according to type size
        switch (type_size) {
            case 1:
                std::cout << +reinterpret_cast<uint8_t*>(entry.data)[idx] << " ";
                break;
            case 2:
                std::cout << *reinterpret_cast<uint16_t*>(reinterpret_cast<uint8_t*>(entry.data) + idx*2) << " ";
                break;
            case 4:
                std::cout << *reinterpret_cast<uint32_t*>(reinterpret_cast<uint8_t*>(entry.data) + idx*4) << " ";
                break;
            case 8:
                std::cout << *reinterpret_cast<uint64_t*>(reinterpret_cast<uint8_t*>(entry.data) + idx*8) << " ";
                break;
            default:
                std::cout << "[raw]";
                break;
        }
    }
    std::cout << "\n";
}

/// Print a list of BIGINTs stored in an IVector
void PrintListBigIntIVector(const IVector &entry) {
    if (!entry.data) {
        std::cout << "IVector is empty.\n";
        return;
    }

    auto list_entries = reinterpret_cast<const list_entry_t *>(entry.data);

    // The child data starts after all list_entry_t structures
    const idx_t entry_bytes = sizeof(list_entry_t) * entry.count;
    auto child_values = reinterpret_cast<const int64_t *>(entry.data + entry_bytes);

    // Construct ValidityMask if needed
    ValidityMask mask;
    if (!entry.is_valid && entry.validity) {
        mask = ValidityMask(entry.validity, entry.count);
    }

    std::cout << "=== IVector (LIST(BIGINT)) Dump ===\n";
    std::cout << "Count: " << entry.count << "\n";
    std::cout << "is_valid: " << entry.is_valid << "\n";

    for (idx_t i = 0; i < entry.count; i++) {
        if (!entry.is_valid && entry.validity && !mask.RowIsValid(i)) {
            std::cout << "Row " << i << ": NULL\n";
            continue;
        }

        const auto &e = list_entries[i];
        std::cout << "Row " << i << " -> [offset=" << e.offset
                  << ", len=" << e.length << "] = { ";

        for (idx_t j = 0; j < e.length; j++) {
            std::cout << child_values[e.offset + j];
            if (j + 1 < e.length) std::cout << ", ";
        }
        std::cout << " }\n";
    }

    std::cout << "=================================\n";
}

/// Materialize a DuckDB LIST(BIGINT) vector into an IVector
void LogListBigIntVector(Vector &vec, idx_t count, IVector &entry) {
    D_ASSERT(vec.GetType().id() == LogicalTypeId::LIST);
    D_ASSERT(ListType::GetChildType(vec.GetType()).id() == LogicalTypeId::BIGINT);

    UnifiedVectorFormat udata;
    vec.ToUnifiedFormat(count, udata);

    entry.count = count;
    entry.is_valid = udata.validity.AllValid();
    entry.sel = nullptr;

    // Get list entries and child data
    auto list_entries = ListVector::GetData(vec);
    idx_t total_elements = ListVector::GetListSize(vec);

    Vector &child = ListVector::GetEntry(vec);
    UnifiedVectorFormat child_data;
    child.ToUnifiedFormat(total_elements, child_data);

    const idx_t entry_bytes = sizeof(list_entry_t) * count;
    const idx_t child_type_size = GetTypeIdSize(child.GetType().InternalType());
    const idx_t child_bytes = child_type_size * total_elements;

    // Optional: copy validity mask
    if (!entry.is_valid) {
        entry.validity_bytes = ValidityMask::ValidityMaskSize(count);
        entry.validity = (validity_t *)malloc(entry.validity_bytes);
        entry.bytes += entry.validity_bytes;
        if (!entry.validity) {
            throw InternalException("LogListBigIntVector: malloc for validity failed");
        }
        memcpy(entry.validity, udata.validity.GetData(), entry.validity_bytes);
    }

    // Allocate one contiguous buffer for [list_entry_t[count]] + [BIGINT[total_elements]]
    entry.data = (data_ptr_t)malloc(entry_bytes + child_bytes);
    entry.bytes += entry_bytes + child_bytes;
    if (!entry.data) {
        throw InternalException("LogListBigIntVector: malloc failed");
    }

    // Layout: [list_entry_t[count]] [child_data[total_elements]]
    data_ptr_t list_buf = entry.data;
    data_ptr_t child_buf = entry.data + entry_bytes;

    // Copy list entries
    memcpy(list_buf, list_entries, entry_bytes);

    // Copy child BIGINTs
    if (child_data.sel && child_data.sel->data()) {
        auto sel = child_data.sel->data();
        for (idx_t i = 0; i < total_elements; i++) {
            const idx_t src_idx = sel[i];
            memcpy(child_buf + i * child_type_size,
                   child_data.data + src_idx * child_type_size,
                   child_type_size);
        }
    } else {
        memcpy(child_buf, child_data.data, child_bytes);
    }

   if (LineageState::debug) PrintListBigIntIVector(entry);
}


/// Materialize a scalar vector into an IVector
void LogVector(Vector &vec, idx_t count, IVector &entry) {
    auto type_size = GetTypeIdSize(vec.GetType().InternalType());
    UnifiedVectorFormat udata;
    vec.ToUnifiedFormat(count, udata);
    entry.count = count;
    entry.is_valid = udata.validity.AllValid();
    entry.data = nullptr;
    entry.sel = nullptr;

    // TODO: else, not valid and not constant, need to copy the validity mask
    if (!entry.is_valid && vec.GetVectorType() ==  VectorType::CONSTANT_VECTOR) {
      return;
    }
    
    // Allocate destination buffer for data
    entry.data = (data_ptr_t)malloc(type_size * count);
    entry.bytes += type_size * count;
    if (!entry.data) {
        throw InternalException("LogVector: malloc failed");
    }

    if (udata.sel && udata.sel->data()) {
      auto sel = udata.sel->data();
      for (idx_t i = 0; i < count; i++) {
          const idx_t src_idx = sel[i];
          memcpy(entry.data + i * type_size, udata.data + src_idx * type_size, type_size);
      }
    } else {
        memcpy(entry.data, udata.data, type_size * count);
    }
    
    if (LineageState::debug) {
      std::cout << entry.sel << " " << entry.is_valid << " ---> " <<  vec.ToString(count) << std::endl;
      PrintLoggedVector(entry, type_size);
    }
}

} // namespace duckdb
