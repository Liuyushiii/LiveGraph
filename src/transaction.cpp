/* Copyright 2020 Guanyu Feng, Tsinghua University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "core/transaction.hpp"
#include "core/edge_iterator.hpp"
#include "core/graph.hpp"

using namespace livegraph;

/**
 * 创建一个新的顶点。
 * @param use_recycled_vertex 指定是否使用回收的顶点 ID。
 * @return 返回新的顶点 ID。
 */
vertex_t Transaction::new_vertex(bool use_recycled_vertex)
{
    check_valid();
    check_writable();

    vertex_t vertex_id;

    // 如果不是批量更新事务且回收顶点 ID 缓存不为空，则从中取出一个顶点 ID。
    if (!batch_update && recycled_vertex_cache.size())
    {
        vertex_id = recycled_vertex_cache.front();
        recycled_vertex_cache.pop_front();
    }
    // 如果不使用回收的顶点 ID 或者无法获取到，则分配一个新的顶点 ID。
    else if (!use_recycled_vertex || (!graph.recycled_vertex_ids.try_pop(vertex_id)))
    {
        vertex_id = graph.vertex_id.fetch_add(1, std::memory_order_relaxed);
    }

    // 对顶点加锁、设置顶点指针和边标签指针为空。
    graph.vertex_futexes[vertex_id].clear();
    graph.vertex_ptrs[vertex_id] = graph.block_manager.NULLPOINTER;
    graph.edge_label_ptrs[vertex_id] = graph.block_manager.NULLPOINTER;

    // 如果不是批量更新事务，则将新顶点 ID 加入缓存中，并记录 WAL 日志。
    if (!batch_update)
    {
        new_vertex_cache.emplace_back(vertex_id);
        ++wal_num_ops();
        wal_append(OPType::NewVertex);
        wal_append(vertex_id);
    }
    return vertex_id;
}


/**
 * 该方法将数据存储在指定的顶点中。
 *
 * @param vertex_id 顶点id
 * @param data 存储的数据
 */
void Transaction::put_vertex(vertex_t vertex_id, std::string_view data)
{
    // 检查当前事务是否有效
    check_valid();
    
    // 检查当前事务是否可写
    check_writable();
    
    // 检查给定的顶点id是否有效
    check_vertex_id(vertex_id);
    
    // 用于记录顶点之前的指针
    uintptr_t prev_pointer;
    
    // 如果是批量更新
    if (batch_update)
    {
        // 对于该顶点获取独占锁
        graph.vertex_futexes[vertex_id].lock();
        
        // 记录顶点之前的指针
        prev_pointer = graph.vertex_ptrs[vertex_id];
    }
    else
    {
        // 如果不是批量更新，则确保在访问该顶点之前已经获取了该顶点的锁
        ensure_vertex_lock(vertex_id);
        
        // 在缓存中查找该顶点的指针
        auto cache_iter = vertex_ptr_cache.find(vertex_id);
        
        // 如果找到了，则使用之前缓存的指针
        if (cache_iter != vertex_ptr_cache.end()) {
            // std::cout << "from cache" << std::endl;
            prev_pointer = cache_iter->second;
        }
        // 如果没有找到，则确保该顶点不存在冲突，然后使用图中的指针
        else
        {
            // std::cout << "not from cache" << std::endl;
            ensure_no_confict(vertex_id);
            prev_pointer = graph.vertex_ptrs[vertex_id];
        }
    }
    
    // 计算存储顶点数据所需的块的大小
    auto size = sizeof(VertexBlockHeader) + data.size();
    
    // 将块的大小转换为对应的阶数
    auto order = size_to_order(size);
    
    // 在块管理器中为该块分配空间
    auto pointer = graph.block_manager.alloc(order);
    
    // 将指针转换为VertexBlockHeader类型的指针
    auto vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
    
    // 使用给定的数据填充VertexBlockHeader并更新相关状态
    vertex_block->fill(order, vertex_id, write_epoch_id, prev_pointer, data.data(), data.size());
    
    // 将该顶点标记为已更新状态
    graph.compact_table.local().emplace(vertex_id);
    
    // 如果是批量更新，则直接更新图中顶点的指针
    if (batch_update)
    {
        graph.vertex_ptrs[vertex_id] = pointer;
        graph.vertex_futexes[vertex_id].unlock();
    }
    else
    {
        // 否则将更新缓存、块缓存和wal日志
        block_cache.emplace_back(pointer, order);
        timestamps_to_update.emplace_back(vertex_block->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);
        vertex_ptr_cache[vertex_id] = pointer;
        ++wal_num_ops();
        wal_append(OPType::PutVertex);
        wal_append(vertex_id);
        wal_append(data);
    }
}

bool Transaction::del_vertex(vertex_t vertex_id, bool recycle)
{
    check_valid();
    check_writable();
    check_vertex_id(vertex_id);

    uintptr_t prev_pointer;
    if (batch_update)
    {
        graph.vertex_futexes[vertex_id].lock();
        prev_pointer = graph.vertex_ptrs[vertex_id];
    }
    else
    {
        ensure_vertex_lock(vertex_id);
        auto cache_iter = vertex_ptr_cache.find(vertex_id);
        if (cache_iter != vertex_ptr_cache.end())
            prev_pointer = cache_iter->second;
        else
        {
            ensure_no_confict(vertex_id);
            prev_pointer = graph.vertex_ptrs[vertex_id];
        }
    }

    bool ret = false;
    auto prev_vertex_block = graph.block_manager.convert<VertexBlockHeader>(prev_pointer);
    if (prev_vertex_block && prev_vertex_block->get_length() != prev_vertex_block->TOMBSTONE)
    {
        ret = true;
        auto size = sizeof(VertexBlockHeader);
        auto order = size_to_order(size);
        auto pointer = graph.block_manager.alloc(order);

        auto vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
        vertex_block->fill(order, vertex_id, write_epoch_id, prev_pointer, nullptr, vertex_block->TOMBSTONE);

        graph.compact_table.local().emplace(vertex_id);

        if (!batch_update)
        {
            block_cache.emplace_back(pointer, order);
            timestamps_to_update.emplace_back(vertex_block->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);
            vertex_ptr_cache[vertex_id] = pointer;
        }
    }

    if (batch_update)
    {
        if (recycle)
            graph.recycled_vertex_ids.push(vertex_id);
        graph.vertex_futexes[vertex_id].unlock();
    }
    else
    {
        ++wal_num_ops();
        wal_append(OPType::DelVertex);
        wal_append(vertex_id);
        wal_append(recycle);

        if (recycle)
            recycled_vertex_cache.emplace_back(vertex_id);
    }

    return ret;
}

std::string_view Transaction::get_vertex(vertex_t vertex_id)
{
    check_valid();

    if (vertex_id >= graph.vertex_id.load(std::memory_order_relaxed))
        return std::string_view();

    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {
        pointer = graph.vertex_ptrs[vertex_id];
    }
    else
    {
        auto cache_iter = vertex_ptr_cache.find(vertex_id);
        if (cache_iter != vertex_ptr_cache.end())
            pointer = cache_iter->second;
        else
            pointer = graph.vertex_ptrs[vertex_id];
    }

    auto vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
    while (vertex_block)
    {
        if (cmp_timestamp(vertex_block->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0)
            break;
        pointer = vertex_block->get_prev_pointer();
        vertex_block = graph.block_manager.convert<VertexBlockHeader>(pointer);
    }

    // if (!(batch_update || !trace_cache))
    //{
    //    vertex_ptr_cache[vertex_id] = pointer;
    //}

    if (!vertex_block || vertex_block->get_length() == vertex_block->TOMBSTONE)
        return std::string_view();

    return std::string_view(vertex_block->get_data(), vertex_block->get_length());
}

std::pair<EdgeEntry *, char *>
Transaction::find_edge(vertex_t dst, EdgeBlockHeader *edge_block, size_t num_entries, size_t data_length)
{
    // bool stop = false;

    if (!edge_block)
        return {nullptr, nullptr};

    auto bloom_filter = edge_block->get_bloom_filter();
    if (bloom_filter.valid() && !bloom_filter.find(dst))
        return {nullptr, nullptr};

    auto entries = edge_block->get_entries() - num_entries;
    auto data = edge_block->get_data() + data_length;

    // EdgeEntry* first = nullptr;
    // char * second = nullptr;

    for (size_t i = 0; i < num_entries; i++)
    {
        data -= entries->get_length();
        // std::cout << "creation_time: " << *entries->get_creation_time_pointer();
        // std::cout << ", deletion_time: " << *entries->get_deletion_time_pointer();
        // std::cout << ", version: " << *entries->get_version_pointer() << std::endl;
        // std::cout << "data: " << data << std::endl;
        if (entries->get_dst() == dst &&
            cmp_timestamp(entries->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0 &&
            cmp_timestamp(entries->get_deletion_time_pointer(), read_epoch_id, local_txn_id) > 0)
        {
            // std::cout << "creation_time: " << *entries->get_creation_time_pointer();
            // std::cout << ", deletion_time: " << *entries->get_deletion_time_pointer();
            // std::cout << ", version: " << *entries->get_version_pointer() << std::endl;
            // std::cout << "data: " << data << std::endl;
            // stop = true;
            // first = entries;
            // second = data;
            return {entries, data};
        }
        entries++;
    }

    return {nullptr, nullptr};
    // return {first, second};
}

// 返回源节点src的与给定标签label对应的边的指针
uintptr_t Transaction::locate_edge_block(vertex_t src, label_t label)
{
    // 获取源节点src的标签指针
    auto pointer = graph.edge_label_ptrs[src];
    // 如果标签指针为空，则直接返回NULL指针
    if (pointer == graph.block_manager.NULLPOINTER)
        return pointer;
    // 将标签指针转换为边标签块头指针
    auto edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(pointer);
    // 遍历边标签块中的所有标签项
    for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
    {
        // 获取标签项
        auto label_entry = edge_label_block->get_entries()[i];
        // 如果标签项的标签值等于要查找的标签label，则查找该标签对应的边
        if (label_entry.get_label() == label)
        {
            // 获取标签项指向的边块指针
            auto pointer = label_entry.get_pointer();
            // 如果边块指针不为空，则遍历所有该标签对应的边块，并返回最后一个块的指针
            while (pointer != graph.block_manager.NULLPOINTER)
            {
                // 将边块指针转换为边块头指针
                auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);
                // 如果边块的创建时间早于等于当前事务的时间，则返回该边块的指针
                if (cmp_timestamp(edge_block->get_creation_time_pointer(), read_epoch_id, local_txn_id) <= 0)
                    break;
                // 否则继续向前遍历
                pointer = edge_block->get_prev_pointer();
            }
            return pointer;
        }
    }
    // 如果找不到对应的边块，则返回NULL指针
    return graph.block_manager.NULLPOINTER;
}


// 保护多个事务同时访问图中的同一条边，以避免写入冲突。它是一个关键的保障措施，以确保系统的数据一致性和正确性。
void Transaction::ensure_no_confict(vertex_t src, label_t label)
{
    // 从图中获取指向src的edge_label_ptr，并检查它是否为 NULLPOINTER。如果是，函数将立即返回，因为没有与该源节点相关的edge_label_block。
    auto pointer = graph.edge_label_ptrs[src];
    if (pointer == graph.block_manager.NULLPOINTER)
        return;
    // 如果存在edge_label_block，则函数遍历其条目并查找与给定标签匹配的条目。
    auto edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(pointer);
    for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
    {
        auto label_entry = edge_label_block->get_entries()[i];
        if (label_entry.get_label() == label)
        {
            auto pointer = label_entry.get_pointer();
            // 如果找到，则获取该条目的指针，并检查它是否为 NULLPOINTER。
            if (pointer != graph.block_manager.NULLPOINTER)
            {
                auto header = graph.block_manager.convert<EdgeBlockHeader>(pointer);
                // 如果不是，则获取该指针所指向的边缘块的头信息，并比较其提交时间戳与当前事务的时间戳，以确定是否存在写入冲突。
                if (header && cmp_timestamp(header->get_committed_time_pointer(), read_epoch_id, local_txn_id) > 0)
                // 如果存在写入冲突，则函数抛出 RollbackExcept 异常，指示发生了写入-写入冲突
                    throw RollbackExcept("Write-write confict on: " + std::to_string(src) + ": " +
                                         std::to_string(label) + ".");
            }
            return;
        }
    }
}

void Transaction::update_edge_label_block(vertex_t src, label_t label, uintptr_t edge_block_pointer)
{
    auto pointer = graph.edge_label_ptrs[src];
    auto edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(pointer);
    if (edge_label_block)
    {
        for (size_t i = 0; i < edge_label_block->get_num_entries(); i++)
        {
            auto &label_entry = edge_label_block->get_entries()[i];
            if (label_entry.get_label() == label)
            {
                label_entry.set_pointer(edge_block_pointer);
                return;
            }
        }
    }

    EdgeLabelEntry label_entry;
    label_entry.set_label(label);
    label_entry.set_pointer(edge_block_pointer);

    if (!edge_label_block || !edge_label_block->append(label_entry))
    {
        auto num_entries = edge_label_block ? edge_label_block->get_num_entries() : 0;
        auto size = sizeof(EdgeLabelBlockHeader) + (1 + num_entries) * sizeof(EdgeLabelEntry);
        auto order = size_to_order(size);

        auto new_pointer = graph.block_manager.alloc(order);

        auto new_edge_label_block = graph.block_manager.convert<EdgeLabelBlockHeader>(new_pointer);
        new_edge_label_block->fill(order, src, write_epoch_id, pointer);

        if (!batch_update)
        {
            block_cache.emplace_back(new_pointer, order);
            timestamps_to_update.emplace_back(new_edge_label_block->get_creation_time_pointer(),
                                              Graph::ROLLBACK_TOMBSTONE);
        }

        for (size_t i = 0; i < num_entries; i++)
        {
            auto old_label_entry = edge_label_block->get_entries()[i];
            new_edge_label_block->append(old_label_entry);
        }

        new_edge_label_block->append(label_entry);

        graph.edge_label_ptrs[src] = new_pointer;
    }
}

void Transaction::put_edge(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data, bool force_insert)
{
    check_valid();
    check_writable();
    check_vertex_id(src);
    check_vertex_id(dst);

    uintptr_t pointer;
    // 是否需要批量更新
    if (batch_update)
    {
        // 对src上锁
        graph.vertex_futexes[src].lock();
        // 定位edge block
        pointer = locate_edge_block(src, label);
    }
    else
    {
        // 对src上锁
        ensure_vertex_lock(src);
        // 在 "edge_ptr_cache" 中查找指向<src, label>的edge block的指针，如果找到了，则将其存储在 "pointer" 变量中。
        auto cache_iter = edge_ptr_cache.find(std::make_pair(src, label));
        if (cache_iter != edge_ptr_cache.end())
        {
            pointer = cache_iter->second;
        }
        else
        {
            // 确保不存在写入不存在冲突
            ensure_no_confict(src, label);
            pointer = locate_edge_block(src, label);
            // 将新的edge block的指针添加到缓存中
            // cancel cache
            edge_ptr_cache.emplace_hint(cache_iter, std::make_pair(src, label), pointer);
        }
    }

    EdgeEntry entry;
    entry.set_length(edge_data.size());
    entry.set_dst(dst);
    // *************************************************
    // creation_time与write_epoch_id一致
    entry.set_creation_time(write_epoch_id);
    entry.set_version(timestamp_t(888));
    entry.set_deletion_time(Graph::ROLLBACK_TOMBSTONE);
    
    
    auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);

    auto [num_entries, data_length] =
        edge_block ? get_num_entries_data_length_cache(edge_block) : std::pair<size_t, size_t>{0, 0};

    if (!edge_block || !edge_block->has_space(entry, num_entries, data_length))
    {
        auto size = sizeof(EdgeBlockHeader) + (1 + num_entries) * sizeof(EdgeEntry) + data_length + entry.get_length();

        std::cout << "size: " << size << std::endl;

        auto order = size_to_order(size);

        if (order > edge_block->BLOOM_FILTER_PORTION &&
            size + (1ul << (order - edge_block->BLOOM_FILTER_PORTION)) >= (1ul << edge_block->BLOOM_FILTER_THRESHOLD))
        {
            size += 1ul << (order - edge_block->BLOOM_FILTER_PORTION);
        }
        order = size_to_order(size);

        auto new_pointer = graph.block_manager.alloc(order);

        auto new_edge_block = graph.block_manager.convert<EdgeBlockHeader>(new_pointer);
        new_edge_block->fill(order, src, write_epoch_id, pointer, write_epoch_id);

        if (!batch_update)
        {
            block_cache.emplace_back(new_pointer, order);
            timestamps_to_update.emplace_back(new_edge_block->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);
            // timestamps_to_update.emplace_back(new_edge_block->get_committed_time_pointer(),
            // Graph::ROLLBACK_TOMBSTONE); update when commit
        }

        if (edge_block)
        {
            auto entries = edge_block->get_entries();
            auto data = edge_block->get_data();

            auto bloom_filter = new_edge_block->get_bloom_filter();
            for (size_t i = 0; i < num_entries; i++)
            {
                entries--;
                // skip deleted edges
                if (cmp_timestamp(entries->get_deletion_time_pointer(), read_epoch_id, local_txn_id) > 0)
                {
                    auto edge = new_edge_block->append(*entries, data, bloom_filter); // direct update size
                    if (!batch_update && edge->get_creation_time() == -local_txn_id)
                        timestamps_to_update.emplace_back(edge->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);
                }
                data += entries->get_length();
            }
        }

        if (batch_update)
            update_edge_label_block(src, label, new_pointer);

        pointer = new_pointer;
        edge_block = new_edge_block;
        std::tie(num_entries, data_length) = new_edge_block->get_num_entries_data_length_atomic();
    }

    if (!force_insert)
    {
        auto prev_edge = find_edge(dst, edge_block, num_entries, data_length);

        if (prev_edge.first)
        {
            prev_edge.first->set_deletion_time(write_epoch_id);
            if (!batch_update)
                timestamps_to_update.emplace_back(prev_edge.first->get_deletion_time_pointer(),
                                                  Graph::ROLLBACK_TOMBSTONE);
        }
    }

    auto edge = edge_block->append_without_update_size(entry, edge_data.data(), num_entries, data_length);
    set_num_entries_data_length_cache(edge_block, num_entries + 1, data_length + entry.get_length());
    if (!batch_update)
        timestamps_to_update.emplace_back(edge->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);

    graph.compact_table.local().emplace(src);

    if (batch_update)
    {
        graph.vertex_futexes[src].unlock();
    }
    else
    {
        // cancel cache
        edge_ptr_cache[std::make_pair(src, label)] = pointer;
        ++wal_num_ops();
        wal_append(OPType::PutEdge);
        wal_append(src);
        wal_append(label);
        wal_append(dst);
        wal_append(force_insert);
        wal_append(edge_data);
    }
}

bool Transaction::del_edge(vertex_t src, label_t label, vertex_t dst)
{
    check_valid();
    check_writable();
    check_vertex_id(src);
    check_vertex_id(dst);

    uintptr_t pointer;
    if (batch_update)
    {
        graph.vertex_futexes[src].lock();
        pointer = locate_edge_block(src, label);
    }
    else
    {
        ensure_vertex_lock(src);
        auto cache_iter = edge_ptr_cache.find(std::make_pair(src, label));
        if (cache_iter != edge_ptr_cache.end())
        {
            pointer = cache_iter->second;
        }
        else
        {
            ensure_no_confict(src, label);
            pointer = locate_edge_block(src, label);
            // cancel cache
            edge_ptr_cache.emplace_hint(cache_iter, std::make_pair(src, label), pointer);
        }
    }

    auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);

    if (!edge_block)
        return false;

    auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);
    auto edge = find_edge(dst, edge_block, num_entries, data_length);

    if (edge.first)
    {
        edge.first->set_deletion_time(write_epoch_id);
        if (!batch_update)
            timestamps_to_update.emplace_back(edge.first->get_deletion_time_pointer(), Graph::ROLLBACK_TOMBSTONE);
    }

    graph.compact_table.local().emplace(src);

    if (batch_update)
    {
        graph.vertex_futexes[src].unlock();
    }
    else
    {
        // cancel cache
        edge_ptr_cache[std::make_pair(src, label)] = pointer;

        // make sure commit will change committed_time
        set_num_entries_data_length_cache(edge_block, num_entries, data_length);

        ++wal_num_ops();
        wal_append(OPType::DelEdge);
        wal_append(src);
        wal_append(label);
        wal_append(dst);
    }

    if (edge.first != nullptr)
        return true;
    else
        return false;
}

std::string_view Transaction::get_edge(vertex_t src, label_t label, vertex_t dst)
{
    check_valid();

    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return std::string_view();

    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {
        pointer = locate_edge_block(src, label);
    }
    else
    {
        auto cache_iter = edge_ptr_cache.find(std::make_pair(src, label));
        if (cache_iter != edge_ptr_cache.end())
        {
            pointer = cache_iter->second;
        }
        else
        {
            pointer = locate_edge_block(src, label);
            // cancel cache
            edge_ptr_cache.emplace_hint(cache_iter, std::make_pair(src, label), pointer);
        }
    }

    auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);

    if (!edge_block)
        return std::string_view();

    auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);
    // {entries, data}
    auto edge = find_edge(dst, edge_block, num_entries, data_length);

    if (edge.first)
        // edge.second: edge data
        // edge.first->get_length(): length of data
        return std::string_view(edge.second, edge.first->get_length());
    else
        return std::string_view();
}

void Transaction::abort()
{
    check_valid();

    for (const auto &p : timestamps_to_update)
    {
        *p.first = p.second;
    }

    for (const auto &vid : new_vertex_cache)
    {
        graph.recycled_vertex_ids.push(vid);
    }

    for (const auto &p : block_cache)
    {
        graph.block_manager.free(p.first, p.second);
    }

    clean();
}

EdgeIterator Transaction::get_edges(vertex_t src, label_t label, bool reverse)
{
    check_valid();

    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return EdgeIterator(nullptr, nullptr, 0, 0, read_epoch_id, local_txn_id, reverse);

    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {
        pointer = locate_edge_block(src, label);
    }
    else
    {
        auto cache_iter = edge_ptr_cache.find(std::make_pair(src, label));
        if (cache_iter != edge_ptr_cache.end())
        {
            pointer = cache_iter->second;
        }
        else
        {
            pointer = locate_edge_block(src, label);
            // cancel cache
            edge_ptr_cache.emplace_hint(cache_iter, std::make_pair(src, label), pointer);
        }
    }

    auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);

    if (!edge_block)
        return EdgeIterator(nullptr, nullptr, 0, 0, read_epoch_id, local_txn_id, reverse);

    auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);

    return EdgeIterator(edge_block->get_entries(), edge_block->get_data(), num_entries, data_length, read_epoch_id,
                        local_txn_id, reverse);
}

timestamp_t Transaction::commit(bool wait_visable)
{
    check_valid();
    check_writable();

    if (batch_update)
        return read_epoch_id;

    auto [commit_epoch_id, num_unfinished] = graph.commit_manager.register_commit(wal);

    for (const auto &p : vertex_ptr_cache)
    {
        auto vertex_id = p.first;
        auto pointer = p.second;
        if (graph.vertex_ptrs[vertex_id] != pointer)
            graph.vertex_ptrs[vertex_id] = pointer;
    }

    for (const auto &vid : recycled_vertex_cache)
    {
        graph.recycled_vertex_ids.push(vid);
    }

    for (const auto &p : edge_block_num_entries_data_length_cache)
    {
        p.first->set_num_entries_data_length_atomic(p.second.first, p.second.second);
        timestamps_to_update.emplace_back(p.first->get_committed_time_pointer(), p.first->get_committed_time());
        p.first->set_committed_time(write_epoch_id);
    }

    for (const auto &p : edge_ptr_cache)
    {
        auto prev_pointer = locate_edge_block(p.first.first, p.first.second);
        if (p.second != prev_pointer)
        {
            update_edge_label_block(p.first.first, p.first.second, p.second);
        }
    }

    for (const auto &p : timestamps_to_update)
    {
        *p.first = commit_epoch_id;
    }

    clean();

    graph.commit_manager.finish_commit(commit_epoch_id, num_unfinished, wait_visable);

    return commit_epoch_id;
}
void Transaction::put_edge_with_version(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data, int version, bool force_insert)
{
    // std::cout << "=====put_edge_with_version=====" << std::endl;
    check_valid();
    check_writable();
    check_vertex_id(src);
    check_vertex_id(dst);

    uintptr_t pointer;
    // 是否需要批量更新
    if (batch_update)
    {
        // 对src上锁
        graph.vertex_futexes[src].lock();
        // 定位edge block
        pointer = locate_edge_block(src, label);
    }
    else
    {
        // 对src上锁
        ensure_vertex_lock(src);
        // 在 "edge_ptr_cache" 中查找指向<src, label>的edge block的指针，如果找到了，则将其存储在 "pointer" 变量中。
        auto cache_iter = edge_ptr_cache.find(std::make_pair(src, label));
        if (cache_iter != edge_ptr_cache.end())
        {
            pointer = cache_iter->second;
        }
        else
        {
            // 确保不存在写入不存在冲突
            ensure_no_confict(src, label);
            pointer = locate_edge_block(src, label);
            // 将新的edge block的指针添加到缓存中
            // cancel cache
            edge_ptr_cache.emplace_hint(cache_iter, std::make_pair(src, label), pointer);
        }
    }

    EdgeEntry entry;
    entry.set_length(edge_data.size());
    entry.set_dst(dst);
    // *************************************************
    // creation_time与write_epoch_id一致
    entry.set_creation_time(write_epoch_id);
    entry.set_version(timestamp_t(version));
    entry.set_deletion_time(Graph::ROLLBACK_TOMBSTONE);
    
    
    auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);

    auto [num_entries, data_length] =
        edge_block ? get_num_entries_data_length_cache(edge_block) : std::pair<size_t, size_t>{0, 0};

    if (!edge_block || !edge_block->has_space(entry, num_entries, data_length))
    {
        // std::cout << "not exist or no space" << std::endl;

        auto size = sizeof(EdgeBlockHeader) + (1 + num_entries) * sizeof(EdgeEntry) + data_length + entry.get_length();
        auto order = size_to_order(size);

        if (order > edge_block->BLOOM_FILTER_PORTION &&
            size + (1ul << (order - edge_block->BLOOM_FILTER_PORTION)) >= (1ul << edge_block->BLOOM_FILTER_THRESHOLD))
        {
            size += 1ul << (order - edge_block->BLOOM_FILTER_PORTION);
        }
        order = size_to_order(size);

        auto new_pointer = graph.block_manager.alloc(order);

        auto new_edge_block = graph.block_manager.convert<EdgeBlockHeader>(new_pointer);
        new_edge_block->fill(order, src, write_epoch_id, pointer, write_epoch_id);

        if (!batch_update)
        {
            block_cache.emplace_back(new_pointer, order);
            timestamps_to_update.emplace_back(new_edge_block->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);
            // timestamps_to_update.emplace_back(new_edge_block->get_committed_time_pointer(),
            // Graph::ROLLBACK_TOMBSTONE); update when commit
        }

        if (edge_block)
        {
            auto entries = edge_block->get_entries();
            auto data = edge_block->get_data();

            auto bloom_filter = new_edge_block->get_bloom_filter();
            // std::cout << "current num entries: " << num_entries << std::endl;
            // std::cout << "init: " << *entries->get_version_pointer() << std::endl;


            for (size_t i = 0; i < num_entries; i++)
            {
                entries--;
                // skip deleted edges
                // std::cout << "here: " << *entries->get_version_pointer() << std::endl;
                if (cmp_timestamp(entries->get_deletion_time_pointer(), read_epoch_id, local_txn_id) > 0)
                {
                    // std::cout << "stop: " << *entries->get_version_pointer() << std::endl;
                    auto edge = new_edge_block->append(*entries, data, bloom_filter); // direct update size
                    if (!batch_update && edge->get_creation_time() == -local_txn_id)
                    {
                        // std::cout << "remain: " << *edge->get_version_pointer() << std::endl;
                        timestamps_to_update.emplace_back(edge->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);
                    }
                }
                else
                    auto edge = new_edge_block->append(*entries, data, bloom_filter);
                data += entries->get_length();
            }
        }
        // else{
        //     std::cout << "edge block not exist" << std::endl;
        // }

        if (batch_update)
            update_edge_label_block(src, label, new_pointer);

        pointer = new_pointer;
        edge_block = new_edge_block;
        std::tie(num_entries, data_length) = new_edge_block->get_num_entries_data_length_atomic();
    }

    if (!force_insert)
    {
        auto prev_edge = find_edge(dst, edge_block, num_entries, data_length);

        if (prev_edge.first)
        {
            prev_edge.first->set_deletion_time(write_epoch_id);
            if (!batch_update)
                timestamps_to_update.emplace_back(prev_edge.first->get_deletion_time_pointer(),
                                                  Graph::ROLLBACK_TOMBSTONE);
        }
    }

    auto edge = edge_block->append_without_update_size(entry, edge_data.data(), num_entries, data_length);
    set_num_entries_data_length_cache(edge_block, num_entries + 1, data_length + entry.get_length());
    if (!batch_update)
        timestamps_to_update.emplace_back(edge->get_creation_time_pointer(), Graph::ROLLBACK_TOMBSTONE);

    graph.compact_table.local().emplace(src);

    if (batch_update)
    {
        graph.vertex_futexes[src].unlock();
    }
    else
    {
        // cancel cache
        edge_ptr_cache[std::make_pair(src, label)] = pointer;
        ++wal_num_ops();
        wal_append(OPType::PutEdge);
        wal_append(src);
        wal_append(label);
        wal_append(dst);
        wal_append(force_insert);
        wal_append(edge_data);
    }
    // std::cout << "==================" << std::endl;
}



std::vector<std::string_view> Transaction::get_edge_with_version(vertex_t src, label_t label, vertex_t dst, timestamp_t start, timestamp_t end)
{

    std::vector<std::string_view> views;

    check_valid();

    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return views;

    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {
        pointer = locate_edge_block(src, label);
    }
    else
    {
        auto cache_iter = edge_ptr_cache.find(std::make_pair(src, label));
        if (cache_iter != edge_ptr_cache.end())
        {
            pointer = cache_iter->second;
        }
        else
        {
            pointer = locate_edge_block(src, label);
            // cancel cache
            edge_ptr_cache.emplace_hint(cache_iter, std::make_pair(src, label), pointer);
        }
        // pointer = locate_edge_block(src, label);
    }

    auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);

    if (!edge_block)
        return views;

    auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);
    // {entries, data}
    std::vector<std::pair<EdgeEntry *, char *>> edges = find_edge_with_version(dst, edge_block, num_entries, data_length, start, end);

    if (edges.size() == 0) {
        return views;
    }
    else {
        for (int i = 0; i < edges.size(); i++) {
            auto edge = edges[i];
            views.push_back(std::string_view(edge.second, edge.first->get_length()));
        }
    }
    return views;

}

std::vector<std::pair<EdgeEntry *, char *>>
Transaction::find_edge_with_version(vertex_t dst, EdgeBlockHeader *edge_block, size_t num_entries, size_t data_length, timestamp_t start, timestamp_t end)
{
    std::vector<std::pair<EdgeEntry *, char *>> edges;

    if (!edge_block)
        return edges;

    auto bloom_filter = edge_block->get_bloom_filter();
    if (bloom_filter.valid() && !bloom_filter.find(dst))
        return edges;

    auto entries = edge_block->get_entries() - num_entries;
    auto data = edge_block->get_data() + data_length;
    for (size_t i = 0; i < num_entries; i++)
    {
        data -= entries->get_length();
        // std::cout << "=================================" << std::endl;
        // std::cout << "creation_time: " << *entries->get_creation_time_pointer();
        // std::cout << ", deletion_time: " << *entries->get_deletion_time_pointer();
        // std::cout << ", version: " << *entries->get_version_pointer() << std::endl;
        // std::cout << "data: " << data << std::endl;
        // std::cout << "=================================" << std::endl;
        if (entries->get_dst() == dst) {
            if (cmp_timestamp(entries->get_version_pointer(), start) >= 0 &&
                cmp_timestamp(entries->get_version_pointer(), end) <= 0) 
                edges.push_back({entries, data});
            // std::cout << "**********************************" << std::endl;
            // std::cout << "creation_time: " << *entries->get_creation_time_pointer();
            // std::cout << ", deletion_time: " << *entries->get_deletion_time_pointer();
            // std::cout << ", version: " << *entries->get_version_pointer() << std::endl;
            // std::cout << "data: " << data << std::endl;
            // std::cout << "**********************************" << std::endl;
        }
        entries++;
    }

    return edges;
}

// EdgeIterator Transaction::get_edges(vertex_t src, label_t label, bool reverse)
EdgeIteratorVersion Transaction::get_edges_with_version(vertex_t src, label_t label, timestamp_t start, timestamp_t end, bool reverse)
{
    // std::clock_t start_time = std::clock();


    
    check_valid();

    if (src >= graph.vertex_id.load(std::memory_order_relaxed))
        return EdgeIteratorVersion(nullptr, nullptr, 0, 0, read_epoch_id, local_txn_id, start, end, reverse);

    uintptr_t pointer;
    if (batch_update || !trace_cache)
    {

        pointer = locate_edge_block(src, label);

    }
    else
    {
        auto cache_iter = edge_ptr_cache.find(std::make_pair(src, label));
        if (cache_iter != edge_ptr_cache.end())
        {
            pointer = cache_iter->second;
        }
        else
        {
            pointer = locate_edge_block(src, label);
            // cancel cache
            edge_ptr_cache.emplace_hint(cache_iter, std::make_pair(src, label), pointer);
        }
        // pointer = locate_edge_block(src, label);
    }

    auto edge_block = graph.block_manager.convert<EdgeBlockHeader>(pointer);

    if (!edge_block)
        return EdgeIteratorVersion(nullptr, nullptr, 0, 0, read_epoch_id, local_txn_id, start, end, reverse);

    auto [num_entries, data_length] = get_num_entries_data_length_cache(edge_block);


    // std::clock_t end_time = std::clock();
    // double elapsed_time = static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC * 1000;
    // std::cout << "part time:" << elapsed_time << std::endl;

    return EdgeIteratorVersion(edge_block->get_entries(), edge_block->get_data(), num_entries, data_length, read_epoch_id,
                        local_txn_id, start, end, reverse);
}

void Transaction::count_size(vertex_t max_vertex_id) {
    // std::cout << "size of edge_label_ptrs: " << sizeof(graph.edge_label_ptrs) / 1024 << " KB" << std::endl;
    // std::cout << "size of edge_ptr_cache: " << sizeof(edge_ptr_cache) / 1024 << " KB" << std::endl;
    // std::cout << "size of vertex_ptrs: " << sizeof(graph.vertex_ptrs) / 1024 << " KB" << std::endl;
    std::cout << "size of edge_label_ptrs: " << max_vertex_id * sizeof(graph.edge_label_ptrs) << " Bytes" << std::endl;
    size_t edge_ptr_cache_size = sizeof(std::map<std::pair<vertex_t, label_t>, uintptr_t>) + edge_ptr_cache.size() * (sizeof(std::pair<vertex_t, label_t>) + sizeof(uintptr_t));
    std::cout << "size of edge_ptr_cache: " << edge_ptr_cache_size << " Bytes" << std::endl;
    std::cout << "size of vertex_ptrs: " << max_vertex_id * sizeof(graph.vertex_ptrs) << " Bytes" << std::endl;
}