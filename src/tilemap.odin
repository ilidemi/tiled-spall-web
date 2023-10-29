package main

import "core:encoding/endian"
import "core:fmt"
import "core:math"
import "core:sort"
import "formats:spall"
import "vendor:wasm/js"

tilemap_v1_load :: proc(trace: ^Trace, chunk: []u8) {
	chunk_pos: uint = size_of(spall.Header)
	process_count := read_u32(chunk, &chunk_pos)
	trace.tilemap.processes = make([dynamic]TiledProcess, big_global_allocator)
	for i in 0..<process_count {
		process_id := read_u32(chunk, &chunk_pos)
		process_min_time := read_i64(chunk, &chunk_pos)

		p_idx := setup_pid(trace, process_id)
		proc_v := &trace.processes[p_idx]
		proc_v.min_time = process_min_time

		if int(p_idx) >= len(trace.tilemap.processes) {
			resize(&trace.tilemap.processes, int(p_idx) + 1)
		}
		tiled_proc := &trace.tilemap.processes[p_idx]
		tiled_proc.pid = process_id
		tiled_proc.threads = make([dynamic]TiledThread, big_global_allocator)

		thread_count := read_u32(chunk, &chunk_pos)
		for j in 0..<thread_count {
			thread_id := read_u32(chunk, &chunk_pos)
			thread_min_time := read_i64(chunk, &chunk_pos)
			thread_max_time := read_i64(chunk, &chunk_pos)
			thread_height := read_u32(chunk, &chunk_pos)

			t_idx := setup_tid(trace, p_idx, thread_id)
			thread := &proc_v.threads[t_idx]
			thread.min_time = thread_min_time
			thread.max_time = thread_max_time
			for k in 0..<thread_height {
				depth := Depth{
					tree = make([dynamic]ChunkNode, big_global_allocator),
					events = make([dynamic]Event, big_global_allocator),
				}
				append(&thread.depths, depth)
			}

			if int(t_idx) >= len(tiled_proc.threads) {
				resize(&tiled_proc.threads, int(t_idx) + 1)
			}
			tiled_thread := &tiled_proc.threads[t_idx]
			tiled_thread.tid = thread_id
			rows_count := int(i_round_up(thread_height, TILE_HEIGHT) / TILE_HEIGHT)
			tiled_thread.rows = make([dynamic]TiledRow, rows_count, big_global_allocator)
			for k in 0..<rows_count {
				tiled_thread.rows[k].zoomed_rows = make([dynamic]TiledZoomedRow, big_global_allocator)
			}
		}
	}

	trace.total_min_time = read_i64(chunk, &chunk_pos)
	trace.total_max_time = read_i64(chunk, &chunk_pos)
	strings_len := read_u32(chunk, &chunk_pos)
	resize(&trace.string_block, int(strings_len))
	copy(trace.string_block[:], chunk[chunk_pos:chunk_pos + uint(strings_len)])
	trace.zoom_event = empty_event
	trace.is_tiled = true

	finish_loading(trace)
}

tilemap_fetch_missing :: proc(trace: ^Trace, visible_ranges: ..VisibleRange) {
	total_duration := trace.total_max_time - trace.total_min_time
	total_duration_log := math.ceil(math.log(f64(total_duration), ZOOM_LOG_BASE))
	total_duration_rounded := i64(math.pow(ZOOM_LOG_BASE, total_duration_log))
	zoom0_time_granularity := total_duration_rounded / INITIAL_NODES_PER_TILE
	for visible_range in visible_ranges {
		time_granularity_ratio := f64(zoom0_time_granularity) / f64(max(visible_range.time_granularity, 1))
		zoom_level := u32(max(1, math.ceil(math.log(time_granularity_ratio, ZOOM_LOG_BASE))))

		for visible_thread in visible_range.threads {
			start_depth_coord := visible_thread.min_depth / TILE_HEIGHT
			end_depth_coord := i_round_up(visible_thread.max_depth, TILE_HEIGHT) / TILE_HEIGHT

			p_idx, _ := vh_find(&trace.process_map, visible_thread.pid)
			tiled_process := &trace.tilemap.processes[visible_thread.pid]

			t_idx, _ := vh_find(&trace.processes[p_idx].thread_map, visible_thread.tid)
			tiled_thread := &tiled_process.threads[t_idx]

			for depth_coord in start_depth_coord..<end_depth_coord {
				row := &tiled_thread.rows[depth_coord]

				if int(zoom_level) >= len(row.zoomed_rows) {
					prev_len := len(row.zoomed_rows)
					resize(&row.zoomed_rows, int(zoom_level) + 1)
					for zoom_i in prev_len..<len(row.zoomed_rows) {
						row.zoomed_rows[zoom_i].tiles = make([dynamic]Tile, big_global_allocator)
					}
				}
				for zoom_i in 0..=zoom_level {
					zoomed_row := &row.zoomed_rows[zoom_i]
					zoomed_tiles_count := len(zoomed_row.tiles)

					zoom_factor := zoom_i * ZOOM_LOG_LOG_BASE
					time_bucket_count := 1 << zoom_factor
					tile_width := total_duration_rounded / i64(time_bucket_count)
					start_time_coord := u32((visible_range.min_time) / tile_width)
					end_time_coord := u32(i_round_up(visible_range.max_time, tile_width) / tile_width)
					end_time_coord = min(end_time_coord, 1 << zoom_factor)

					tile_idx := find_tile_by_time_coord(zoomed_row.tiles, start_time_coord)
					appended := false
					for time_coord in start_time_coord..<end_time_coord {
						if tile_idx < zoomed_tiles_count && time_coord == zoomed_row.tiles[tile_idx].time_coord {
							tile_idx += 1
							continue
						}

						needs_fetching := true
						if zoom_i > 0 {
							parent_time_coord := time_coord / ZOOM_LOG_BASE
							parent_tiles := &row.zoomed_rows[zoom_i - 1].tiles
							parent_tile_idx := find_tile_by_time_coord(parent_tiles^, parent_time_coord)
							needs_fetching = parent_tiles[parent_tile_idx].status != .NotFound
						}
						tile := Tile{
							status = needs_fetching ? .Fetching : .NotFound,
							pid = visible_thread.pid,
							tid = visible_thread.tid,
							depth_coord = depth_coord,
							zoom = zoom_i,
							time_coord = u32(time_coord),
						}
						append(&zoomed_row.tiles, tile)
						appended = true
						if needs_fetching {
							fetch_tile(default_config_url, visible_thread.pid, visible_thread.tid, depth_coord, zoom_i, time_coord)
						}
					}

					if appended {
						tilemap_sort_tiles(&zoomed_row.tiles)
					}
				}
			}
		}
	}
}

tilemap_sort_tiles :: proc(tiles: ^[dynamic]Tile) {
	sort.quick_sort_proc(tiles[:], proc(a, b: Tile) -> int {
		return int(a.time_coord) - int(b.time_coord)
	})
}

@export
tilemap_insert :: proc "contextless" (pid, tid, depth_coord, zoom, time_coord: u32, data: []u8) {
	context = wasmContext
	start_bench("insert tile")
	p_idx, _ := vh_find(&_trace.process_map, pid)
	tiled_process := &_trace.tilemap.processes[pid]

	t_idx, _ := vh_find(&_trace.processes[p_idx].thread_map, tid)
	tiled_thread := &tiled_process.threads[t_idx]
	tiled_row := &tiled_thread.rows[depth_coord]

	tiles := &tiled_row.zoomed_rows[zoom].tiles
	tile_idx := find_tile_by_time_coord(tiles^, time_coord)
	if zoom > 0 {
		parent_time_coord := time_coord / ZOOM_LOG_BASE
		parent_tiles := &tiled_row.zoomed_rows[zoom - 1].tiles
		parent_tile_idx := find_tile_by_time_coord(parent_tiles^, parent_time_coord)
		parent_status := parent_tiles[parent_tile_idx].status
		if parent_status != .Loaded && parent_status != .NotFound && parent_status != .CoveredByParent {
			tiles[tile_idx].status = .WaitingForParent
			if len(data) > 0 {
				tiles[tile_idx].maybe_data = new_clone(data, big_global_allocator)
			}
			return
		}
	}

	process := &_trace.processes[p_idx]
	thread := &process.threads[t_idx]
	events_count_by_depth: [TILE_HEIGHT]int
	for i in 0..<TILE_HEIGHT {
		depth_idx := int(depth_coord) * TILE_HEIGHT + i
		if depth_idx >= len(thread.depths) {
			break
		}
		events_count_by_depth[i] = len(thread.depths[depth_idx].events)
	}
	if len(data) > 0 {
		tilemap_merge(thread, pid, tid, depth_coord, zoom, time_coord, data, tiled_row)
		tiles[tile_idx].status = .Loaded
	} else if tiles[tile_idx].status != .CoveredByParent {
		tiles[tile_idx].status = .NotFound
	}
	if zoom + 1 < u32(len(tiled_row.zoomed_rows)) {
		for child_i in 0..<u32(ZOOM_LOG_BASE) {
			tilemap_insert_recursively(tiled_row, pid, tid, depth_coord, zoom + 1, time_coord * ZOOM_LOG_BASE + child_i, thread)
		}
	}
	for i in 0..<TILE_HEIGHT {
		depth_idx := int(depth_coord) * TILE_HEIGHT + i
		if depth_idx >= len(thread.depths) {
			break
		}
		depth := &thread.depths[depth_idx]
		if events_count_by_depth[i] != len(depth.events) {
			chunk_depth_events(&_trace, thread, depth)
		}
	}
	stop_bench("insert tile")
}

tilemap_insert_recursively :: proc(tiled_row: ^TiledRow, pid, tid, depth_coord, zoom, time_coord: u32, thread: ^Thread) {
	tiles := &tiled_row.zoomed_rows[zoom].tiles
	tile_idx := find_tile_by_time_coord(tiles^, time_coord)
	if tile_idx < len(tiles) &&
		tiles[tile_idx].time_coord == time_coord &&
		(tiles[tile_idx].status == .WaitingForParent || tiles[tile_idx].status == .CoveredByParent) {

		if tiles[tile_idx].status == .WaitingForParent {
			if tiles[tile_idx].maybe_data != nil {
				tilemap_merge(thread, pid, tid, depth_coord, zoom, time_coord, tiles[tile_idx].maybe_data^, tiled_row)
				free(tiles[tile_idx].maybe_data, big_global_allocator)
				tiles[tile_idx].status = .Loaded
			} else {
				tiles[tile_idx].status = .NotFound
			}
		}
		if zoom + 1 < u32(len(tiled_row.zoomed_rows)) {
			for child_i in 0..<u32(ZOOM_LOG_BASE) {
				tilemap_insert_recursively(tiled_row, pid, tid, depth_coord, zoom + 1, time_coord * ZOOM_LOG_BASE + child_i, thread)
			}
		}
	}
}

tilemap_merge :: proc(thread: ^Thread, pid, tid, depth_coord, zoom, time_coord: u32, data: []u8, tiled_row: ^TiledRow) {
	pos: uint = 0
	zoom_levels_covered := u32(read_u8(data, &pos))
	for i in 0..<TILE_HEIGHT {
		new_events_count := read_u64(data, &pos)
		if new_events_count == 0 {
			continue
		}

		new_events: [dynamic]Event
		resize(&new_events, int(new_events_count))
		last_timestamp := i64(0)
		for j in 0..<new_events_count {
			new_events[j].timestamp = last_timestamp + read_i64(data, &pos)
			last_timestamp = new_events[j].timestamp
		}
		for j in 0..<new_events_count {
			new_events[j].duration = read_i64(data, &pos)
		}
		for j in 0..<new_events_count {
			new_events[j].name = read_u32(data, &pos)
		}
		for j in 0..<new_events_count {
			new_events[j].args = read_u32(data, &pos)
		}
		for j in 0..<new_events_count {
			new_events[j].color.r = read_f32(data, &pos)
			new_events[j].color.g = read_f32(data, &pos)
			new_events[j].color.b = read_f32(data, &pos)
		}
		for j in 0..<new_events_count {
			new_events[j].weight = read_i64(data, &pos)
		}
		for j in 0..<new_events_count {
			new_events[j].count = read_i64(data, &pos)
			new_events[j].is_terminal = new_events[j].count == 1
		}

		min_new_timestamp := max(i64)
		max_new_timestamp := min(i64)
		for new_event in new_events {
			min_new_timestamp = min(min_new_timestamp, new_event.timestamp)
			max_new_timestamp = max(max_new_timestamp, new_event.timestamp + new_event.duration)
		}

		events := &thread.depths[int(depth_coord) * TILE_HEIGHT + i].events
		if len(events) == 0 {
			append_elems(events, ..new_events[:])
			continue
		}

		delta_count := 0
		{
			new_idx := 0
			for event_idx in 0..<len(events) {
				if new_idx >= len(new_events) {
					break
				}
				if events[event_idx].timestamp == new_events[new_idx].timestamp {
					new_idx += 1
					for new_idx < len(new_events) &&
						new_events[new_idx].timestamp + new_events[new_idx].duration <=
							events[event_idx].timestamp + events[event_idx].duration {

						new_idx += 1
						delta_count += 1
					}
				}
			}
			if new_idx != len(new_events) {
				fmt.printf("New events don't match, only processed %d/%d\n", new_idx, len(new_events))
			}
		}

		start_idx := 0
		for events[start_idx].timestamp < new_events[0].timestamp {
			start_idx += 1
		}
		old_len := len(events)
		new_len := old_len + delta_count
		rounded_new_len := 1 << uint(math.ceil(math.log2(f64(new_len))))
		reserve(events, rounded_new_len)
		resize(events, new_len)
		copy(events[start_idx+delta_count:], events[start_idx:])
		old_idx := start_idx + delta_count
		new_idx := 0
		for event_idx in start_idx..<len(events) {
			old_event := &events[old_idx]
			new_event := &new_events[new_idx]
			if old_event.timestamp + old_event.duration <= new_event.timestamp {
				// Break between the new events, just fast forward through the old
				events[event_idx] = old_event^
				old_idx += 1
			} else if old_event.timestamp <= new_event.timestamp &&
				old_event.timestamp + old_event.duration >= new_event.timestamp + new_event.duration {

				// New event subdivides the old event, use that
				events[event_idx] = new_event^
				if new_event.timestamp + new_event.duration == old_event.timestamp + old_event.duration {
					old_idx += 1
				}
				new_idx += 1

				if new_idx >= len(new_events) {
					// Remainder is just old events which are already in place
					break
				}
			} else if old_event.timestamp >= new_event.timestamp &&
				old_event.timestamp + old_event.duration <= new_event.timestamp + new_event.duration {

				// Old event subdivides the new event, use that
				// This can happen when the tile already merged had more zoom levels in it and is more granular
				events[event_idx] = old_event^
				if new_event.timestamp + new_event.duration == old_event.timestamp + old_event.duration {
					new_idx += 1
				}
				old_idx += 1

				if new_idx >= len(new_events) {
					// Remainder is just old events which are already in place
					break
				}
			}
		}
	}

	max_zoom_covered := int(zoom + zoom_levels_covered - 1)
	if max_zoom_covered >= len(tiled_row.zoomed_rows) {
		resize(&tiled_row.zoomed_rows, max_zoom_covered + 1)
	}
	for zoom_delta in 1..<zoom_levels_covered {
		child_zoom := zoom + zoom_delta
		zoom_factor := zoom_delta << u32(ZOOM_LOG_LOG_BASE)
		zoom_tiles := &tiled_row.zoomed_rows[child_zoom].tiles
		zoom_tiles_count := len(zoom_tiles)
		child_tile_idx := find_tile_by_time_coord(zoom_tiles^, time_coord * zoom_factor)
		appended := false
		for child_i in 0..<zoom_factor {
			child_time_coord := time_coord * zoom_factor + child_i
			if child_tile_idx < zoom_tiles_count {
				if zoom_tiles[child_tile_idx].time_coord == child_time_coord {
					zoom_tiles[child_tile_idx].status = .CoveredByParent
					child_tile_idx += 1
					continue
				}
			}

			child_tile := Tile{
				status = .CoveredByParent,
				pid = pid,
				tid = tid,
				depth_coord = depth_coord,
				zoom = child_zoom,
				time_coord = child_time_coord,
			}
			append(zoom_tiles, child_tile)
			appended = true
		}
		if appended {
			tilemap_sort_tiles(zoom_tiles)
		}
	}
}

find_tile_by_time_coord :: proc(tiles: [dynamic] Tile, time_coord: u32) -> int {
	tiles1 := tiles
	_ = tiles1
	time_coord1 := time_coord
	_ = time_coord1
	if len(tiles) == 0 || tiles[0].time_coord > time_coord {
		return 0
	} else if tiles[len(tiles) - 1].time_coord < time_coord {
		return len(tiles)
	} else {
		left_idx := 0
		right_idx := len(tiles) - 1
		for left_idx <= right_idx {
			mid_idx := (left_idx + right_idx) / 2
			mid_time_coord := tiles[mid_idx].time_coord
			if mid_time_coord < time_coord {
				left_idx = mid_idx + 1
			} else if mid_time_coord > time_coord {
				right_idx = mid_idx - 1
			} else {
				return mid_idx
			}
		}
		return left_idx
	}
}

read_u8 :: proc(chunk: []u8, pos: ^uint) -> u8 {
	result := chunk[pos^]
	pos^ += 1
	return result
}

read_i32 :: proc(chunk: []u8, pos: ^uint) -> i32 {
	result := (cast(^i32)&chunk[pos^])^
	pos^ += 4
	return result
}

read_u32 :: proc(chunk: []u8, pos: ^uint) -> u32 {
	result := (cast(^u32)&chunk[pos^])^
	pos^ += 4
	return result
}

read_i64 :: proc(chunk: []u8, pos: ^uint) -> i64 {
	result := (cast(^i64)&chunk[pos^])^
	pos^ += 8
	return result
}

read_u64 :: proc(chunk: []u8, pos: ^uint) -> u64 {
	result := (cast(^u64)&chunk[pos^])^
	pos^ += 8
	return result
}

read_f32 :: proc(chunk: []u8, pos: ^uint) -> f32 {
	result := (cast(^f32)&chunk[pos^])^
	pos^ += 4
	return result
}