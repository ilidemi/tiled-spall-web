package tile

import "core:encoding/endian"
import "core:encoding/json"
import "core:fmt"
import "core:math"
import "core:math/linalg/glsl"
import "core:math/rand"
import "core:mem"
import "core:mem/virtual"
import "core:os"
import "core:c/libc"
import "core:runtime"
import "core:slice"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"
import "formats:spall"

JsonTrace :: struct {
	displayTimeUnit: string,
	otherData: map[string]any,
	traceEvents: []JsonEvent,
}
JsonEvent :: struct{
	cat:  string,
	dur:  f64,
	name: string,
	ph:   string,
	pid:  u32,
	tid:  u32,
	ts:   f64,
}

BinEventType :: enum u8 {
	Unknown = 0,
	Instant,
	Complete,
	Begin,
	End,
	Metadata,
	Sample,
}
BinEventScope :: enum u8 {
	Global,
	Process,
	Thread,
}
TempBinEvent :: struct {
	type: BinEventType,
	scope: BinEventScope,
	duration: i64,
	timestamp: i64,
	thread_id: u32,
	process_id: u32,
	name: u32,
	args: u32,
}

Trace :: struct {
	processes: [dynamic]Process,
	process_map: map[u32]uint,
	string_block: [dynamic]u8,
	string_offsets: map[string]u32,
	timestamp_unit: f64,
	min_timestamp: i64,
	max_timestamp: i64,
	min_duration: i64,
}

Process :: struct {
	id: u32,
	min_time: i64,
	threads: [dynamic]Thread,
	thread_map: map[u32]uint,
}

Thread :: struct {
	id: u32,
	min_time: i64,
	max_time: i64,
	depths: [dynamic]Depth,
	current_depth: int,
	bande_q: Stack(EVData)
}

EVData :: struct {
	idx: i32,
	depth: u16,
	self_time: i64,
}
Stack :: struct($T: typeid) {
	arr: [dynamic]T,
	len: int,
}
stack_init :: proc(s: ^$Q/Stack($T), allocator := context.allocator) {
	s.arr = make([dynamic]T, 16, allocator)
	s.len = 0
}
stack_push_back :: proc(s: ^$Q/Stack($T), elem: T) #no_bounds_check {
	if s.len >= cap(s.arr) {
		new_capacity := max(8, len(s.arr)*2)
		resize(&s.arr, new_capacity)
	}
	s.arr[s.len] = elem
	s.len += 1
}
stack_pop_back :: proc(s: ^$Q/Stack($T)) -> T #no_bounds_check {
	s.len -= 1
	return s.arr[s.len]
}
stack_peek_back :: proc(s: ^$Q/Stack($T)) -> T #no_bounds_check { return s.arr[s.len - 1] }
stack_clear :: proc(s: ^$Q/Stack($T)) { s.len = 0 }
print_stack :: proc(s: ^$Q/Stack($T)) {
	fmt.printf("Stack{{\n")
	for i:= 0; i < s.len; i += 1 {
		fmt.printf("%#v\n", s.arr[i])
	}
	fmt.printf("}}\n")
}

TILE_HEIGHT :: 16

Depth :: struct {
	events: [dynamic]Event,
}

Event :: struct {
	timestamp: i64,
	duration: i64,
	name_offset: u32,
	args_offset: u32,
}

TileNode :: struct {
	start_time: i64,
	end_time: i64,
	name_offset: u32,
	args_offset: u32,
	avg_color: FVec3,
	weight: i64,
	count: i64,
}

FVec3 :: [3]f32

INITIAL_NODES_PER_TILE :: 4096
ZOOM_LOG_LOG_BASE :: 3
ZOOM_LOG_BASE :: 1 << ZOOM_LOG_LOG_BASE

main :: proc() {
	if !(len(os.args) == 2 || (len(os.args) == 3 && !strings.has_suffix(os.args[1], ".dump") && os.args[2] == "--dump")) {
		fmt.eprintf("%v <trace.spall((.dump))> (--dump)\n", os.args[0])
		os.exit(1)
	}

	should_make_dump := len(os.args) == 3
	should_load_dump := strings.has_suffix(os.args[1], ".dump")

	total_stopwatch: time.Stopwatch
	time.stopwatch_start(&total_stopwatch)

	trace: Trace
	if should_load_dump {
		trace = load_dump(os.args[1])
	} else {
		data, ok := os.read_entire_file(os.args[1])
		if !ok {
			fmt.eprintf("%v could not be opened for reading.\n", os.args[1])
			os.exit(1)
		}
		defer delete(data)
		if data[0] == '{' {
			trace = load_json_trace(data)
		} else {
			trace = load_binary_trace(data)
		}
	} 
	if should_make_dump {
		make_dump(&trace, os.args[1])
		fmt.println("Dumped the trace")
	}
	fmt.println("Processed the trace")

	starting_duration_rounded := i64(INITIAL_NODES_PER_TILE)
	starting_duration_log := i64(math.log(f64(starting_duration_rounded), ZOOM_LOG_BASE))
	total_duration := trace.max_timestamp - trace.min_timestamp
	total_duration_log := math.ceil(math.log(f64(total_duration), ZOOM_LOG_BASE))
	total_duration_rounded := i64(math.pow(ZOOM_LOG_BASE, total_duration_log))
	min_duration_log := math.floor(math.log(f64(trace.min_duration), ZOOM_LOG_BASE))
	min_duration_rounded := i64(math.pow(ZOOM_LOG_BASE, min_duration_log))
	possible_zoom_levels := i64(total_duration_log - min_duration_log + 1)
	zoom_levels := max(1, possible_zoom_levels - starting_duration_log)
	fmt.printf(
		"Total duration: %d, min duration: %d, zoom levels: %d\n",
		total_duration_rounded, min_duration_rounded, zoom_levels,
	)

	color_choices: [COLORS_COUNT]FVec3
	rand.set_global_seed(1)
	hsv2rgb :: proc(c: FVec3) -> FVec3 {
		K := glsl.vec3{1.0, 2.0 / 3.0, 1.0 / 3.0}
		sum := glsl.vec3{c.x, c.x, c.x} + K.xyz
		p := glsl.abs_vec3(glsl.fract(sum) * 6.0 - glsl.vec3{3,3,3})
		result := glsl.vec3{c.z, c.z, c.z} * glsl.mix(K.xxx, glsl.clamp(p - K.xxx, 0.0, 1.0), glsl.vec3{c.y, c.y, c.y})
		return FVec3{result.x, result.y, result.z}
	}
	for i in 0..<len(color_choices) {
		h := rand.float32() * 0.5 + 0.5
		h *= h
		h *= h
		h *= h
		s := 0.5 + rand.float32() * 0.1
		v : f32 = 0.85

		color_choices[i] = hsv2rgb(FVec3{h, s, v}) * 255
	}


	out_dir := fmt.tprintf("%v.tiles", os.args[1])
	if os.exists(out_dir) {
		rmdir_stopwatch: time.Stopwatch
		time.stopwatch_start(&rmdir_stopwatch)
		cmd := fmt.tprintf("rmdir /s /q %s", out_dir)
		if libc.system(strings.clone_to_cstring(cmd)) != 0 {
			fmt.eprintf("Couldn't remove dir %v\n", out_dir)
			os.exit(1)
		}
		rmdir_duration := time.stopwatch_duration(rmdir_stopwatch)
		fmt.printf("rmdir duration: %v\n", rmdir_duration)
	}
	make_directory_or_exit(out_dir)

	version: u64 = 1
	tile_map_buf: [dynamic]u8
	append_u64(&tile_map_buf, spall.TILEMAP_MAGIC)
	append_u64(&tile_map_buf, version)
	append_f64(&tile_map_buf, trace.timestamp_unit)
	append_u64(&tile_map_buf, 0)
	append_u32(&tile_map_buf, u32(len(trace.processes)))

	target_duration := total_duration_rounded / starting_duration_rounded;
	total_tile_count := 0

	tasks: [dynamic]TaskData
	globals := TaskGlobals{
		out_dir = out_dir,
		zoom_levels = zoom_levels,
		total_duration_rounded = total_duration_rounded,
		starting_duration_rounded = starting_duration_rounded,
		min_timestamp = trace.min_timestamp,
		max_timestamp = trace.max_timestamp,
		color_choices = &color_choices,
		total_tile_count = &total_tile_count,
	}
	for _, p_idx in trace.processes {
		process := &trace.processes[p_idx]
		process_dir := fmt.tprintf("%s/%d", out_dir, process.id)
		make_directory_or_exit(process_dir)

		append_u32(&tile_map_buf, process.id)
		append_i64(&tile_map_buf, process.min_time)
		append_u32(&tile_map_buf, u32(len(process.threads)))

		for _, t_idx in process.threads {
			thread := &process.threads[t_idx]
			thread_dir := fmt.tprintf("%s/%d", process_dir, thread.id)
			make_directory_or_exit(thread_dir)

			append_u32(&tile_map_buf, thread.id)
			append_i64(&tile_map_buf, thread.min_time)
			append_i64(&tile_map_buf, thread.max_time)
			append_u32(&tile_map_buf, u32(len(thread.depths)))

			for row_depth := 0; row_depth < len(thread.depths); row_depth += TILE_HEIGHT {
				depth_dir := fmt.tprintf("%s/%d", thread_dir, row_depth / TILE_HEIGHT)
				make_directory_or_exit(depth_dir)

				append(&tasks, TaskData{
					row = Row{
						process = process,
						thread = thread,
						depth = row_depth,
					},
					globals = &globals,
				})
			}
		}
	}
	fmt.printf("%d tasks\n", len(tasks))

	pool: thread.Pool
	thread.pool_init(&pool, context.allocator, 6)
	defer thread.pool_destroy(&pool)
	for _, task_idx in tasks {
		thread.pool_add_task(&pool, context.allocator, make_row_tiles, &tasks[task_idx], task_idx)
	}
	thread.pool_start(&pool)
	thread.pool_finish(&pool)

	append_i64(&tile_map_buf, trace.min_timestamp)
	append_i64(&tile_map_buf, trace.max_timestamp)
	append_u32(&tile_map_buf, u32(len(trace.string_block)))
	for i in 0..<len(trace.string_block) {
		append_u8(&tile_map_buf, trace.string_block[i])
	}

	tile_map_path := fmt.tprintf("%s/spalltilemap", out_dir)
	if !os.write_entire_file(tile_map_path, tile_map_buf[:]) {
		fmt.eprintf("Coulnd't write file %v\n", tile_map_path)
		os.exit(1)
	}
	fmt.println(tile_map_path)

	fmt.printf("Tiles: %d\n", total_tile_count)
	total_time := time.stopwatch_duration(total_stopwatch)
	fmt.printf("Total time: %v\n", total_time)
}

TaskData :: struct {
	row: Row,
	globals: ^TaskGlobals,
}

Row :: struct {
	process: ^Process,
	thread: ^Thread,
	depth: int,
}

COLORS_COUNT :: 16

TaskGlobals :: struct {
	out_dir: string,
	zoom_levels: i64,
	total_duration_rounded: i64,
	starting_duration_rounded: i64,
	min_timestamp: i64,
	max_timestamp: i64,
	color_choices: ^[COLORS_COUNT]FVec3,
	total_tile_count: ^int
}

make_row_tiles :: proc(task: thread.Task) {
	task_data := (^TaskData)(task.data)
	row := task_data.row
	globals := task_data.globals
	depth_dir := fmt.tprintf("%s/%d/%d/%d", globals.out_dir, row.process.id, row.thread.id, row.depth / TILE_HEIGHT)
	task_stopwatch: time.Stopwatch
	time.stopwatch_start(&task_stopwatch)
	fmt.printf("Task %s started\n", depth_dir)
	defer {
		elapsed := time.stopwatch_duration(task_stopwatch)
		fmt.printf("Task %s finished in %v\n", depth_dir, elapsed)
	}

	TerminatedInterval :: struct {
		start_time: i64,
		end_time: i64,
	}
	terminated_intervals: [dynamic]TerminatedInterval
	defer delete(terminated_intervals)

	tile_nodes_by_depth_by_zoom: [dynamic][TILE_HEIGHT][dynamic]TileNode
	defer {
		for tile_nodes_by_depth in tile_nodes_by_depth_by_zoom {
			for tile_nodes in tile_nodes_by_depth {
				delete(tile_nodes)
			}
		}
		delete(tile_nodes_by_depth_by_zoom)
	}
	coarsening_durations: [dynamic]time.Duration
	defer delete(coarsening_durations)
	for zoom_level in 0..<globals.zoom_levels {
		zoom_stopwatch: time.Stopwatch
		time.stopwatch_start(&zoom_stopwatch)
		defer {
			elapsed := time.stopwatch_duration(zoom_stopwatch)
			append(&coarsening_durations, elapsed)
		}

		parent_tile_nodes_by_depth: ^[TILE_HEIGHT][dynamic]TileNode
		if zoom_level > 0 {
			parent_tile_nodes_by_depth = &tile_nodes_by_depth_by_zoom[zoom_level - 1]
		}
		tile_nodes_by_depth: [TILE_HEIGHT][dynamic]TileNode
		is_terminal := true
		for tile_depth in 0..<TILE_HEIGHT {
			depth_idx := row.depth + tile_depth
			if depth_idx >= len(row.thread.depths) {
				continue
			}

			events := row.thread.depths[depth_idx].events
			tile_nodes := &tile_nodes_by_depth[tile_depth]
			parent_tile_nodes: ^[dynamic]TileNode
			if parent_tile_nodes_by_depth != nil {
				parent_tile_nodes = &parent_tile_nodes_by_depth[tile_depth]
				reserve(tile_nodes, len(parent_tile_nodes))
			}

			time_coord_count := 1 << (uint(zoom_level) * ZOOM_LOG_LOG_BASE)
			tile_width := globals.total_duration_rounded / i64(time_coord_count)
			time_granularity :=	tile_width / globals.starting_duration_rounded

			current_node: TileNode
			parent_node_idx := 0
			for event in events {
				end_time := event.timestamp + event.duration
				if current_node.count > 0 && end_time - current_node.start_time > time_granularity {
					current_node.avg_color /= f32(current_node.weight)
					append(tile_nodes, current_node)
					current_node = TileNode{}
				}

				current_node.end_time = end_time
				color_choice := event.name_offset % len(globals.color_choices)
				current_node.avg_color += globals.color_choices[color_choice] * f32(event.duration)
				current_node.weight += event.duration
				current_node.count += 1
				if current_node.count == 1 {
					current_node.start_time = event.timestamp
					current_node.name_offset = event.name_offset
					current_node.args_offset = event.args_offset
				} else {
					is_terminal = false
					current_node.name_offset = 0
					current_node.args_offset = 0
				}

				if parent_tile_nodes != nil && parent_tile_nodes[parent_node_idx].end_time == end_time {
					current_node.avg_color /= f32(current_node.weight)
					append(tile_nodes, current_node)
					current_node = TileNode{}
					parent_node_idx += 1
				}
			}

			if current_node.count > 0 {
				current_node.avg_color /= f32(current_node.weight)
				append(tile_nodes, current_node)
			}
		}
		append(&tile_nodes_by_depth_by_zoom, tile_nodes_by_depth)

		if is_terminal {
			break
		}
	}

	coarsening_builder: strings.Builder
	fmt.sbprintf(&coarsening_builder, "%s coarsening ", depth_dir)
	for duration, zoom_level in coarsening_durations {
		fmt.sbprintf(&coarsening_builder, "%d:%v ", zoom_level, duration)
	}
	fmt.println(strings.to_string(coarsening_builder))
	strings.builder_destroy(&coarsening_builder)

	SkipTileKey :: struct {
		zoom_level: int,
		time_coord: int,
	}
	tiles_to_skip: map[SkipTileKey]bool
	defer delete(tiles_to_skip)

	buf: [dynamic]u8
	defer delete(buf)

	validated_tile_nodes_by_depth: [TILE_HEIGHT][dynamic]^TileNode
	defer {
		for validated_tile_nodes in validated_tile_nodes_by_depth {
			delete(validated_tile_nodes)
		}
	}

	for _, zoom_level in tile_nodes_by_depth_by_zoom {
		zoom_dir := fmt.tprintf("%s/%d", depth_dir, zoom_level)
		make_directory_or_exit(zoom_dir)

		tile_count := 1 << (uint(zoom_level) * ZOOM_LOG_LOG_BASE)
		tile_width := globals.total_duration_rounded / i64(tile_count)

		tile_nodes_by_depth := &tile_nodes_by_depth_by_zoom[zoom_level]
		tile_node_indices_by_depth: [TILE_HEIGHT]int
		parent_tile_node_indices_by_depth: [TILE_HEIGHT]int
		terminated_interval_idx: int
		prev_terminated_intervals_count := len(terminated_intervals)
		parent_tile_nodes_by_depth: ^[TILE_HEIGHT][dynamic]TileNode
		if zoom_level > 0 {
			parent_tile_nodes_by_depth = &tile_nodes_by_depth_by_zoom[zoom_level - 1]
		}

		OutTile :: struct {
			time_coord: int,
			start_time: i64,
			end_time: i64,
			zoom_levels_covered: u8,
			out_tile_nodes_by_depth: [TILE_HEIGHT][dynamic]^TileNode,
		}
		out_tiles: [dynamic]OutTile
		defer delete(out_tiles)

		check_skipped_stopwatch: time.Stopwatch
		merge_stopwatch: time.Stopwatch
		binary_search_stopwatch: time.Stopwatch
		find_end_index_stopwatch: time.Stopwatch
		filter_stopwatch: time.Stopwatch
		append_tile_stopwatch: time.Stopwatch
		append_buf_stopwatch: time.Stopwatch
		file_write_stopwatch: time.Stopwatch

		for time_coord in 0..<tile_count {
			tile_start_time := globals.min_timestamp + i64(time_coord) * tile_width
			tile_end_time := tile_start_time + tile_width

			{
				time.stopwatch_start(&check_skipped_stopwatch)
				defer time.stopwatch_stop(&check_skipped_stopwatch)

				skip_key := SkipTileKey{
					zoom_level = zoom_level,
					time_coord = time_coord,
				}
				if tiles_to_skip[skip_key] {
					for tile_depth in 0..<TILE_HEIGHT {
						tile_nodes := &tile_nodes_by_depth[tile_depth]
						tile_node_idx := &tile_node_indices_by_depth[tile_depth]
						if parent_tile_nodes_by_depth == nil {
							for tile_node_idx^ < len(tile_nodes) &&
								tile_nodes[tile_node_idx^].end_time < tile_end_time {

								tile_node_idx^ += 1
							}
						} else {
							parent_tile_nodes := &parent_tile_nodes_by_depth[tile_depth]
							parent_tile_node_idx := &parent_tile_node_indices_by_depth[tile_depth]
							for parent_tile_node_idx^ < len(parent_tile_nodes) &&
								parent_tile_nodes[parent_tile_node_idx^].end_time < tile_end_time {

								parent_tile_node_idx^ += 1
							}
							if parent_tile_node_idx^ < len(parent_tile_nodes) {
								parent_start_time := parent_tile_nodes[parent_tile_node_idx^].start_time
								for tile_node_idx^ < len(tile_nodes) &&
									tile_nodes[tile_node_idx^].start_time < parent_start_time {

									tile_node_idx^ += 1
								}
							} else {
								tile_node_idx^ = len(tile_nodes)
							}
						}
					}
					for terminated_interval_idx < prev_terminated_intervals_count &&
						terminated_intervals[terminated_interval_idx].end_time < tile_end_time {

						terminated_interval_idx += 1
					}
					continue
				}
				if terminated_interval_idx < prev_terminated_intervals_count {
					if tile_start_time >= terminated_intervals[terminated_interval_idx].end_time {
						terminated_interval_idx += 1
					}
					if terminated_interval_idx < prev_terminated_intervals_count &&
						tile_start_time >= terminated_intervals[terminated_interval_idx].start_time &&
						tile_start_time < terminated_intervals[terminated_interval_idx].end_time {

						continue
					}
				}
				if tile_start_time > globals.max_timestamp {
					terminated_interval := TerminatedInterval{
						start_time = tile_start_time,
						end_time = tile_end_time,
					}
					append(&terminated_intervals, terminated_interval)
					continue
				}
			}

			time.stopwatch_start(&merge_stopwatch)
			out_tile := OutTile{
				time_coord = time_coord,
				start_time = tile_start_time,
				end_time = tile_end_time,
			}
			orig_parent_tile_node_indices_by_depth := parent_tile_node_indices_by_depth
			is_terminal := true
			for merge_zoom := zoom_level; ; merge_zoom += 1 {
				out_tile.zoom_levels_covered = u8(merge_zoom - zoom_level + 1)
				merge_is_terminal := true

				tile_nodes_by_depth := &tile_nodes_by_depth_by_zoom[merge_zoom]
				merge_tile_node_indices_by_depth: ^[TILE_HEIGHT]int
				merge_parent_tile_node_indices_by_depth: ^[TILE_HEIGHT]int
				merge_tile_node_indices_by_depth_val: [TILE_HEIGHT]int
				merge_parent_tile_node_indices_by_depth_val: [TILE_HEIGHT]int
				if merge_zoom == zoom_level {
					merge_tile_node_indices_by_depth = &tile_node_indices_by_depth
					merge_parent_tile_node_indices_by_depth = &parent_tile_node_indices_by_depth
				} else {
					events_binary_search :: proc(
						tile_nodes: ^[dynamic]TileNode, timestamp: i64,
						merge_stopwatch, binary_search_stopwatch: ^time.Stopwatch,
					) -> int {
						time.stopwatch_stop(merge_stopwatch)
						defer time.stopwatch_start(merge_stopwatch)

						time.stopwatch_start(binary_search_stopwatch)
						defer time.stopwatch_stop(binary_search_stopwatch)

						if len(tile_nodes) == 0 {
							return 0
						}
						if tile_nodes[len(tile_nodes) - 1].start_time < timestamp {
							return len(tile_nodes)
						}
						low_idx := 0
						high_idx := len(tile_nodes) - 1
						for low_idx <= high_idx {
							mid_idx := (low_idx + high_idx) / 2
							if tile_nodes[mid_idx].start_time == timestamp {
								return mid_idx
							} else if tile_nodes[mid_idx].start_time > timestamp {
								high_idx = mid_idx - 1
							} else {
								low_idx = mid_idx + 1
							}
						}
						return high_idx + 1
					}
					for tile_depth in 0..<TILE_HEIGHT {
						merge_tile_node_indices_by_depth_val[tile_depth] = events_binary_search(
							&tile_nodes_by_depth[tile_depth], tile_start_time,
							&merge_stopwatch, &binary_search_stopwatch,
						)
					}
					merge_tile_node_indices_by_depth = &merge_tile_node_indices_by_depth_val
					merge_parent_tile_node_indices_by_depth_val = orig_parent_tile_node_indices_by_depth
					merge_parent_tile_node_indices_by_depth = &merge_parent_tile_node_indices_by_depth_val
				}

				filtered_tile_nodes_by_depth: [TILE_HEIGHT][dynamic]^TileNode
				for tile_depth in 0..<TILE_HEIGHT {
					start_idx := merge_tile_node_indices_by_depth[tile_depth]
					tile_nodes := &tile_nodes_by_depth[tile_depth]
					if start_idx >= len(tile_nodes) {
						continue
					}

					if tile_nodes[start_idx].start_time >= tile_end_time {
						continue
					}

					time.stopwatch_start(&find_end_index_stopwatch)
					end_idx := start_idx
					for end_idx < len(tile_nodes) - 1 &&
						tile_nodes[end_idx + 1].start_time < tile_end_time {

						end_idx += 1
					}
					node_count := end_idx - start_idx + 1
					if tile_nodes[end_idx].end_time < tile_end_time {
						end_idx += 1
					}
					merge_tile_node_indices_by_depth[tile_depth] = end_idx
					time.stopwatch_stop(&find_end_index_stopwatch)

					time.stopwatch_start(&filter_stopwatch)
					filtered_tile_nodes := &filtered_tile_nodes_by_depth[tile_depth]
					if parent_tile_nodes_by_depth == nil {
						for tile_idx := start_idx; tile_idx < start_idx + node_count; tile_idx += 1 {
							append(filtered_tile_nodes, &tile_nodes[tile_idx])
							if tile_nodes[tile_idx].count > 1 {
								merge_is_terminal = false
							}
						}
					} else {
						parent_tile_nodes := parent_tile_nodes_by_depth[tile_depth]
						parent_idx := &merge_parent_tile_node_indices_by_depth[tile_depth]

						for tile_nodes[start_idx].start_time > parent_tile_nodes[parent_idx^].start_time {
							start_idx -= 1
							node_count += 1
						}

						for tile_idx := start_idx; ; tile_idx += 1 {
							parent_tile_node := &parent_tile_nodes[parent_idx^]
							tile_node := &tile_nodes[tile_idx]
							if tile_node.count > 1 {
								merge_is_terminal = false
							}
							if !(tile_node.start_time == parent_tile_node.start_time &&
								tile_node.end_time == parent_tile_node.end_time) {

								append(filtered_tile_nodes, tile_node)
							}
							if tile_node.end_time == parent_tile_node.end_time {
								if parent_tile_node.end_time < tile_end_time {
									parent_idx^ += 1
								}
								if tile_idx >= start_idx + node_count - 1 {
									break
								}
							}
						}
					}
					time.stopwatch_stop(&filter_stopwatch)
				}

				// Thresholds don't account for counters but tile nodes are the main contributor to file size
				TARGET_TILE_NODE_COUNT :: 1024 * 1024 / size_of(TileNode)
				MAX_TILE_NODE_COUNT :: 8 * 1024 * 1024 / size_of(TileNode)
				total_tile_node_count := 0
				for filtered_tile_nodes in filtered_tile_nodes_by_depth {
					total_tile_node_count += len(filtered_tile_nodes)
				}
				if merge_zoom != zoom_level && total_tile_node_count > MAX_TILE_NODE_COUNT {
					// Exceeded max count, discard this iteration
					for filtered_tile_nodes in filtered_tile_nodes_by_depth {
						delete(filtered_tile_nodes)
					}
					break
				} else {
					// Keep the results of this iteration, continue or finish if the target is reached
					for out_tile_nodes in out_tile.out_tile_nodes_by_depth {
						delete(out_tile_nodes)
					}
					out_tile.out_tile_nodes_by_depth = filtered_tile_nodes_by_depth

					if merge_zoom != zoom_level {
						skipped_count := 1 << ((out_tile.zoom_levels_covered - 1) * ZOOM_LOG_LOG_BASE)
						skipped_start_coord := time_coord * skipped_count
						for skipped_coord in skipped_start_coord..<skipped_start_coord+skipped_count {
							key := SkipTileKey{
								zoom_level = merge_zoom,
								time_coord = skipped_coord,
							}
							tiles_to_skip[key] = true
						}
					}

					is_terminal = merge_is_terminal
					if merge_is_terminal || total_tile_node_count >= TARGET_TILE_NODE_COUNT {
						break
					}
				}
			}
			time.stopwatch_stop(&merge_stopwatch)

			time.stopwatch_start(&append_tile_stopwatch)
			out_tile_node_count := 0
			for out_tile_nodes in out_tile.out_tile_nodes_by_depth {
				out_tile_node_count += len(out_tile_nodes)
			}
			if is_terminal {
				terminated_interval := TerminatedInterval{tile_start_time, tile_end_time}
				for ti in terminated_intervals {
					if terminated_interval.end_time > ti.start_time && terminated_interval.start_time < ti.end_time {
						fmt.eprintf("%s: overlapping termiated intervals %d-%d and %d-%d\n", zoom_dir, ti.start_time, ti.end_time, terminated_interval.start_time, terminated_interval.end_time)
						os.exit(1)
					}
				}
				append(&terminated_intervals, terminated_interval)
				if out_tile_node_count == 0 {
					continue
				}
			}
			append(&out_tiles, out_tile)
			time.stopwatch_stop(&append_tile_stopwatch)

			time.stopwatch_start(&append_buf_stopwatch)
			resize(&buf, 0)
			reserve(&buf, 1 + TILE_HEIGHT * (size_of(u64) + out_tile_node_count))
			append_u8(&buf, out_tile.zoom_levels_covered)
			for tile_depth in 0..<TILE_HEIGHT {
				out_tile_nodes := &out_tile.out_tile_nodes_by_depth[tile_depth]
				append_u64(&buf, u64(len(out_tile_nodes)))
				last_start_time := i64(0)
				for tile_node in out_tile_nodes {
					append_i64(&buf, tile_node.start_time - last_start_time)
					last_start_time = tile_node.start_time
				}
				for tile_node in out_tile_nodes {
					append_i64(&buf, tile_node.end_time - tile_node.start_time)
				}
				for tile_node in out_tile_nodes {
					append_u32(&buf, tile_node.name_offset)
				}
				for tile_node in out_tile_nodes {
					append_u32(&buf, tile_node.args_offset)
				}
				for tile_node in out_tile_nodes {
					append_f32(&buf, tile_node.avg_color[0])
					append_f32(&buf, tile_node.avg_color[1])
					append_f32(&buf, tile_node.avg_color[2])
				}
				for tile_node in out_tile_nodes {
					append_i64(&buf, tile_node.weight)
				}
				for tile_node in out_tile_nodes {
					append_i64(&buf, tile_node.count)
				}
			}
			time.stopwatch_stop(&append_buf_stopwatch)

			time.stopwatch_start(&file_write_stopwatch)
			tile_path := fmt.tprintf("%s/%d.spalltile", zoom_dir, time_coord)
			if !os.write_entire_file(tile_path, buf[:]) {
				fmt.eprintf("Coulnd't write file %v\n", tile_path)
				os.exit(1)
			}
			time.stopwatch_stop(&file_write_stopwatch)
			sync.atomic_add(globals.total_tile_count, 1)
		}

		update_terminated_stopwatch: time.Stopwatch
		time.stopwatch_start(&update_terminated_stopwatch)
		slice.sort_by_key(terminated_intervals[:], proc(ti: TerminatedInterval) -> i64 {
			return ti.start_time;
		})
		all_terminated := false
		if len(terminated_intervals) > 0 {
			dst_interval_idx := 0
			for src_interval_idx in 1..<len(terminated_intervals) {
				src_interval := &terminated_intervals[src_interval_idx]
				dst_interval := &terminated_intervals[dst_interval_idx]
				if dst_interval.end_time == src_interval.start_time {
					dst_interval.end_time = src_interval.end_time
				} else {
					dst_interval_idx += 1
					terminated_intervals[dst_interval_idx] = src_interval^
				}
			}
			resize(&terminated_intervals, dst_interval_idx + 1)
			if terminated_intervals[0].start_time == globals.min_timestamp {
				last_interval := terminated_intervals[len(terminated_intervals) - 1]
				if last_interval.end_time == globals.min_timestamp + globals.total_duration_rounded {
					all_terminated = true
					for i in 0..<(len(terminated_intervals) - 1) {
						if terminated_intervals[i].end_time != terminated_intervals[i + 1].start_time {
							all_terminated = false
							break
						}
					}
				}
			}
		}
		if zoom_level == int(globals.zoom_levels - 1) && !all_terminated {
			fmt.eprintf("%s: expected every tile to be terminated at zoom level %d\n", zoom_dir, zoom_level)
			fmt.eprintf("terminated_intervals: ")
			for terminated_interval in terminated_intervals {
				fmt.eprintf("%d-%d ", terminated_interval.start_time, terminated_interval.end_time)
			}
			fmt.eprintln()
			os.exit(1)
		}
		update_terminated_duration := time.stopwatch_duration(update_terminated_stopwatch)

		validate_stopwatch: time.Stopwatch
		time.stopwatch_start(&validate_stopwatch)
		tmp_tile_nodes: [dynamic]^TileNode
		defer delete(tmp_tile_nodes)
		for tile_depth in 0..<TILE_HEIGHT {
			validated_tile_nodes := &validated_tile_nodes_by_depth[tile_depth]
			if parent_tile_nodes_by_depth == nil {
				root_tile_nodes := out_tiles[0].out_tile_nodes_by_depth[tile_depth]
				validated_tile_nodes^ = make([dynamic]^TileNode, len(root_tile_nodes), cap(root_tile_nodes))
				copy(validated_tile_nodes[:], root_tile_nodes[:])
				continue
			}

			tile_nodes_mod3: [3][dynamic]^TileNode
			defer delete(tile_nodes_mod3[0])
			defer delete(tile_nodes_mod3[1])
			defer delete(tile_nodes_mod3[2])
			for tile, tile_idx in out_tiles {
				dst := &tile_nodes_mod3[tile_idx % 3]
				src := tile.out_tile_nodes_by_depth[tile_depth]
				if len(dst) > 0 && len(src) > 0 && dst[len(dst) - 1].end_time > src[0].start_time {
					left := &out_tiles[tile_idx - 3]
					right := &out_tiles[tile_idx]
					fmt.eprintf(
						"%s, depth %d, zoom %d, tiles are not supposed to overlap: %d(%d-%d), %d(%d-%d)\n",
						depth_dir, tile_depth, zoom_level,
						left.time_coord, left.start_time, left.end_time,
						right.time_coord, right.start_time, right.end_time,
					)
					fmt.eprintln()
					for tile_node in out_tiles[tile_idx - 3].out_tile_nodes_by_depth[tile_depth] {
						fmt.eprintf("%d-%d(%d) ", tile_node.start_time, tile_node.end_time, tile_node.count)
					}
					fmt.eprintln()
					for tile_node in out_tiles[tile_idx].out_tile_nodes_by_depth[tile_depth] {
						fmt.eprintf("%d-%d(%d) ", tile_node.start_time, tile_node.end_time, tile_node.count)
					}
					fmt.eprintln()
					os.exit(1)
				}
				append_elems(dst, ..src[:])
			}

			// Merge each tile without neighbors
			for i in 0..<3 {
				reserve(&tmp_tile_nodes, len(validated_tile_nodes))
				resize(&tmp_tile_nodes, len(validated_tile_nodes))
				copy(tmp_tile_nodes[:], validated_tile_nodes[:])

				new_tile_nodes := tile_nodes_mod3[i]
				processed_count := merge_tile(&tmp_tile_nodes, new_tile_nodes)
				if processed_count != len(new_tile_nodes) {
					fmt.eprintf(
						"%s, depth %d, zoom %d, i %d (single): new events don't match, only processed %d/%d\n",
						depth_dir, tile_depth, zoom_level, i, processed_count, len(new_tile_nodes),
					)
					os.exit(1)
				}
			}

			// Merge left to right
			reserve(&tmp_tile_nodes, len(validated_tile_nodes))
			resize(&tmp_tile_nodes, len(validated_tile_nodes))
			copy(tmp_tile_nodes[:], validated_tile_nodes[:])
			for i in 0..<3 {
				new_tile_nodes := tile_nodes_mod3[i]
				processed_count := merge_tile(&tmp_tile_nodes, new_tile_nodes)
				if processed_count != len(new_tile_nodes) {
					fmt.eprintf(
						"%s, depth %d, zoom %d, i %d (ltr): new events don't match, only processed %d/%d\n",
						depth_dir, tile_depth, zoom_level, i, processed_count, len(new_tile_nodes),
					)
					os.exit(1)
				}
			}

			// Merge right to left
			for i := 2; i >= 0; i -= 1 {
				new_tile_nodes := tile_nodes_mod3[i]
				processed_count := merge_tile(validated_tile_nodes, new_tile_nodes)
				if processed_count != len(new_tile_nodes) {
					fmt.eprintf(
						"%s, depth %d, zoom %d, i %d (rtl): new events don't match, only processed %d/%d\n",
						depth_dir, tile_depth, zoom_level, i, processed_count, len(new_tile_nodes),
					)
					os.exit(1)
				}
			}

			if len(tmp_tile_nodes) != len(validated_tile_nodes) {
				fmt.eprintf(
					"%s, depth %d, zoom %d: ltr count %d != rtl count %d\n\n",
					depth_dir, tile_depth, zoom_level, len(tmp_tile_nodes), len(validated_tile_nodes),
				)

				os.exit(1)
			}
			for tile_idx in 0..<len(validated_tile_nodes) {
				ltr_tile_node := tmp_tile_nodes[tile_idx]
				rtl_tile_node := validated_tile_nodes[tile_idx]
				if ltr_tile_node.start_time != rtl_tile_node.start_time ||
					ltr_tile_node.end_time != rtl_tile_node.end_time {
					fmt.eprintf(
						"%s, depth %d, zoom %d: ltr and rtl results differ at idx %d: %d-%d != %d-%d",
						depth_dir, tile_depth, zoom_level, tile_idx, ltr_tile_node.start_time,
						ltr_tile_node.end_time, rtl_tile_node.start_time, rtl_tile_node.end_time,
					)
					os.exit(1)
				}
			}
		}
		validate_duration := time.stopwatch_duration(validate_stopwatch)

		total_check_skipped_duration := time.stopwatch_duration(check_skipped_stopwatch)
		total_binary_search_duration := time.stopwatch_duration(binary_search_stopwatch)
		total_merge_duration := time.stopwatch_duration(merge_stopwatch)
		total_find_end_index_duration := time.stopwatch_duration(find_end_index_stopwatch)
		total_filter_duration := time.stopwatch_duration(filter_stopwatch)
		total_append_tile_duration := time.stopwatch_duration(append_tile_stopwatch)
		total_append_buf_duration := time.stopwatch_duration(append_buf_stopwatch)
		total_file_write_duration := time.stopwatch_duration(file_write_stopwatch)
		fmt.printf(
			"%s: check skipped %v, binary search %v, merge coarsening %v, find end %v, filter %v, append tile %v, append buf %v, I/O %v, update terminated %v, validate %v\n",
			zoom_dir, total_check_skipped_duration, total_binary_search_duration, total_merge_duration,
			total_find_end_index_duration, total_filter_duration, total_append_tile_duration,
			total_append_buf_duration, total_file_write_duration, update_terminated_duration, validate_duration,
		)
		if all_terminated {
			fmt.printf("%s: all terminated\n", zoom_dir)
			break
		}
	}
}

merge_tile :: proc(old_tile_nodes: ^[dynamic]^TileNode, new_tile_nodes: [dynamic]^TileNode) -> int {
	if len(new_tile_nodes) == 0 {
		return 0
	}

	delta_count := 0
	{
		new_idx := 0
		for old_idx in 0..<len(old_tile_nodes) {
			if new_idx >= len(new_tile_nodes) {
				break
			}
			if new_tile_nodes[new_idx].start_time == old_tile_nodes[old_idx].start_time {
				new_idx += 1
				for new_idx < len(new_tile_nodes) &&
					new_tile_nodes[new_idx].end_time <= old_tile_nodes[old_idx].end_time {

					new_idx += 1
					delta_count += 1
				}
			}
		}
		if new_idx != len(new_tile_nodes) {
			return new_idx
		}
	}

	start_idx := 0
	for old_tile_nodes[start_idx].start_time < new_tile_nodes[0].start_time {
		start_idx += 1
	}
	resize(old_tile_nodes, len(old_tile_nodes) + delta_count)
	copy(old_tile_nodes[start_idx+delta_count:], old_tile_nodes[start_idx:])
	old_idx := start_idx + delta_count
	new_idx := 0
	for tile_node_idx in start_idx..<len(old_tile_nodes) {
		old_tile_node := old_tile_nodes[old_idx]
		new_tile_node := new_tile_nodes[new_idx]
		if old_tile_node.end_time <= new_tile_node.start_time {
			// Break between the new nodes, just fast forward through the old
			old_tile_nodes[tile_node_idx] = old_tile_node
			old_idx += 1
		} else if old_tile_node.start_time <= new_tile_node.start_time &&
			old_tile_node.end_time >= new_tile_node.end_time {

			// New node subdivides the old node, use that
			old_tile_nodes[tile_node_idx] = new_tile_node
			if new_tile_node.end_time == old_tile_node.end_time {
				old_idx += 1
			}
			new_idx += 1

			if new_idx >= len(new_tile_nodes) {
				// Remainder is just old nodes which are already in place
				break
			}
		} else if old_tile_node.start_time >= new_tile_node.start_time &&
			old_tile_node.end_time <= new_tile_node.end_time {

			// Old node subdivides the new node, use that
			// This can happen when the tile already merged had more zoom levels in it and is more granular
			old_tile_nodes[tile_node_idx] = old_tile_node
			if new_tile_node.end_time == old_tile_node.end_time {
				new_idx += 1
			}
			old_idx += 1

			if new_idx >= len(new_tile_nodes) {
				// Remainder is just old nodes which are already in place
				break
			}
		} else {
			fmt.eprintf("this is not supposed to happen\n")
			os.exit(1)
		}
	}

	return len(new_tile_nodes)
}

make_dump :: proc(trace: ^Trace, trace_path: string) {
	dump_path := fmt.tprintf("%s.dump", trace_path)
	dump, err := os.open(dump_path, os.O_CREATE)
	if err != 0 {
		fmt.eprintf("Coulnd't open file %s: %v\n", dump_path, err)
		os.exit(1)
	}
	defer {
		if err := os.close(dump); err != 0 {
			fmt.eprintf("Couldn't close file %s: %v\n", dump_path, err)
			os.exit(1)
		}
	}
	processes_len := len(trace.processes)
	os.write_ptr(dump, &processes_len, size_of(processes_len))
	for _, p_idx in trace.processes {
		process := &trace.processes[p_idx]
		os.write_ptr(dump, &process.id, size_of(process.id))
		os.write_ptr(dump, &process.min_time, size_of(process.min_time))
		threads_len := len(process.threads)
		os.write_ptr(dump, &threads_len, size_of(threads_len))
		for _, t_idx in process.threads {
			thread := &process.threads[t_idx]
			os.write_ptr(dump, &thread.id, size_of(thread.id))
			os.write_ptr(dump, &thread.min_time, size_of(thread.min_time))
			os.write_ptr(dump, &thread.max_time, size_of(thread.max_time))
			depths_len := len(thread.depths)
			os.write_ptr(dump, &depths_len, size_of(depths_len))
			for _, d_idx in thread.depths {
				depth := &thread.depths[d_idx]
				events_len := len(depth.events)
				os.write_ptr(dump, &events_len, size_of(events_len))
				os.write_ptr(dump, &depth.events[0], events_len * size_of(Event))
			}
		}
	}
	string_block_len := len(trace.string_block)
	os.write_ptr(dump, &string_block_len, size_of(string_block_len))
	os.write_ptr(dump, &trace.string_block[0], string_block_len)
	string_offsets_len := len(trace.string_offsets)
	os.write_ptr(dump, &string_offsets_len, size_of(string_offsets_len))
	for key, value in trace.string_offsets {
		key := key
		value := value
		key_len := len(key)
		os.write_ptr(dump, &key_len, size_of(key_len))
		if key_len > 0 {
			os.write_ptr(dump, &key, key_len)
		}
		os.write_ptr(dump, &value, size_of(value))
	}
	os.write_ptr(dump, &trace.timestamp_unit, size_of(trace.timestamp_unit))
	os.write_ptr(dump, &trace.min_timestamp, size_of(trace.min_timestamp))
	os.write_ptr(dump, &trace.max_timestamp, size_of(trace.max_timestamp))
	os.write_ptr(dump, &trace.min_duration, size_of(trace.min_duration))
}

load_dump :: proc(dump_path: string) -> Trace {
	dump, err := os.open(dump_path, os.O_RDONLY)
	if err != 0 {
		fmt.eprintf("Coulnd't open file %s: %v\n", dump_path, err)
		os.exit(1)
	}
	defer {
		if err := os.close(dump); err != 0 {
			fmt.eprintf("Couldn't close file %s: %v\n", dump_path, err)
			os.exit(1)
		}
	}

	trace: Trace
	processes_len: int
	os.read_ptr(dump, &processes_len, size_of(processes_len))
	resize(&trace.processes, processes_len)
	for p_idx in 0..<processes_len {
		process := &trace.processes[p_idx]
		os.read_ptr(dump, &process.id, size_of(process.id))
		os.read_ptr(dump, &process.min_time, size_of(process.min_time))
		threads_len: int
		os.read_ptr(dump, &threads_len, size_of(threads_len))
		resize(&process.threads, threads_len)
		for t_idx in 0..<threads_len {
			thread := &process.threads[t_idx]
			os.read_ptr(dump, &thread.id, size_of(thread.id))
			os.read_ptr(dump, &thread.min_time, size_of(thread.min_time))
			os.read_ptr(dump, &thread.max_time, size_of(thread.max_time))
			depths_len: int
			os.read_ptr(dump, &depths_len, size_of(depths_len))
			resize(&thread.depths, depths_len)
			for d_idx in 0..<depths_len {
				depth := &thread.depths[d_idx]
				events_len: int
				os.read_ptr(dump, &events_len, size_of(events_len))
				resize(&depth.events, events_len)
				os.read_ptr(dump, &depth.events[0], events_len * size_of(Event))
			}
		}
	}
	string_block_len: int
	os.read_ptr(dump, &string_block_len, size_of(string_block_len))
	resize(&trace.string_block, string_block_len)
	os.read_ptr(dump, &trace.string_block[0], string_block_len)
	string_offsets_len: int
	os.read_ptr(dump, &string_offsets_len, size_of(string_offsets_len))
	for kv_idx in 0..<string_offsets_len {
		key: string
		value: u32
		key_len: int
		key_val: [dynamic]u8
		os.read_ptr(dump, &key_len, size_of(key_len))
		if key_len > 0 {
			resize(&key_val, key_len)
			os.read_ptr(dump, &key_val[0], key_len)
		}
		key = string(key_val[:])
		os.read_ptr(dump, &value, size_of(value))
		trace.string_offsets[key] = value
	}
	os.read_ptr(dump, &trace.timestamp_unit, size_of(trace.timestamp_unit))
	os.read_ptr(dump, &trace.min_timestamp, size_of(trace.min_timestamp))
	os.read_ptr(dump, &trace.max_timestamp, size_of(trace.max_timestamp))
	os.read_ptr(dump, &trace.min_duration, size_of(trace.min_duration))

	return trace
}

load_json_trace :: proc(data: []byte) -> Trace {
	json_trace: JsonTrace
	if json.unmarshal(data, &json_trace) != nil {
		fmt.eprintf("%v could not be parsed as an event trace.\n", os.args[1])
		os.exit(1)
	}

	slice.sort_by(json_trace.traceEvents, proc(a, b: JsonEvent) -> bool {
		return a.ts < b.ts
	})

	trace: Trace
	intern_init(&trace)
	if json_trace.displayTimeUnit == "ns" {
		trace.timestamp_unit = 1000
	} else {
		trace.timestamp_unit = 1
	}
	trace.min_timestamp = i64(json_trace.traceEvents[0].ts)
	trace.max_timestamp = i64(json_trace.traceEvents[0].ts + json_trace.traceEvents[0].dur)
	trace.min_duration = i64(json_trace.traceEvents[0].dur)

	for json_event in json_trace.traceEvents {
		trace.max_timestamp = max(trace.max_timestamp, i64(json_event.ts + json_event.dur))
		trace.min_duration = min(trace.min_duration, i64(json_event.dur))

		name_len := min(len(json_event.name), 255)
		name := json_event.name[:name_len]
		name_offset := intern_string(&trace, name)

		process := &trace.processes[setup_pid(&trace, json_event.pid)]
		process.min_time = min(process.min_time, i64(json_event.ts))

		thread := &process.threads[setup_tid(process, json_event.tid)]
		thread.min_time = min(thread.min_time, i64(json_event.ts))
		thread.max_time = max(thread.max_time, i64(json_event.ts))

		// Not handling event types other than "X"

		depth: ^Depth
		for _, i in thread.depths {
			current_depth := &thread.depths[i]
			if len(current_depth.events) == 0 {
				depth = current_depth
				break
			}

			last_event := current_depth.events[len(current_depth.events) - 1]
			if i64(json_event.ts) >= last_event.timestamp + last_event.duration {
				depth = current_depth
				break
			}
		}
		if depth == nil {
			append(&thread.depths, Depth{})
			depth = &thread.depths[len(thread.depths) - 1]
		}

		event := Event{
			timestamp = i64(json_event.ts),
			duration = i64(json_event.dur),
			name_offset = name_offset,
		}

		append(&depth.events, event)
	}

	return trace
}

load_binary_trace :: proc(data: []byte) -> Trace {
	header_sz := size_of(spall.Header)
	if len(data) < header_sz {
		fmt.eprintf("Uh, you passed me an empty file?\n")
		os.exit(1)
	}

	magic := (^u64)(raw_data(data))^
	if magic != spall.MANUAL_MAGIC {
		fmt.eprintf("Magic doesn't match: %x\n", magic)
		os.exit(1)
	}

	hdr := cast(^spall.Header)raw_data(data)
	if hdr.version != 1 {
		fmt.printf("Your file version (%d) is not supported!\n", hdr.version)
		os.exit(1)
	}

	trace: Trace
	intern_init(&trace)
	trace.timestamp_unit = hdr.timestamp_unit
	trace.min_duration = max(i64)
	trace.min_timestamp = max(i64)
	trace.max_timestamp = min(i64)
	temp_ev := TempBinEvent{}
	ev := Event{}
	pos := header_sz
	load_loop: for pos < len(data) {
		mem.zero(&temp_ev, size_of(TempBinEvent))
		state := ms_v1_get_next_event(data, &pos, &trace, &temp_ev)

		#partial switch state {
		case .Failure:
			fmt.eprintf("Invalid file\n")
			os.exit(1)
		}

		#partial switch temp_ev.type {
		case .Begin:
			ev.name_offset = temp_ev.name
			ev.args_offset = temp_ev.args
			ev.duration = -1
			ev.timestamp = temp_ev.timestamp

			p_idx, t_idx, e_idx := ms_v1_bin_push_event(&trace, temp_ev.process_id, temp_ev.thread_id, &ev)

			thread := &trace.processes[p_idx].threads[t_idx]
			stack_push_back(&thread.bande_q, EVData{idx = i32(e_idx), depth = u16(thread.current_depth - 1), self_time = 0})
		case .End:
			p_idx, ok1 := trace.process_map[temp_ev.process_id]
			if !ok1 {
				fmt.printf("invalid end?\n")
				continue
			}
			t_idx, ok2 := trace.processes[p_idx].thread_map[temp_ev.thread_id]
			if !ok2 {
				fmt.printf("invalid end?\n")
				continue
			}

			thread := &trace.processes[p_idx].threads[t_idx]
			if thread.bande_q.len > 0 {
				jev_data := stack_pop_back(&thread.bande_q)
				thread.current_depth -= 1

				depth := &thread.depths[thread.current_depth]
				jev := &depth.events[jev_data.idx]
				jev.duration = temp_ev.timestamp - jev.timestamp
				thread.max_time = max(thread.max_time, jev.timestamp + jev.duration)
				trace.max_timestamp = max(trace.max_timestamp, jev.timestamp + jev.duration)
				trace.min_duration = min(trace.min_duration, jev.duration)

				if thread.bande_q.len > 0 {
					parent_depth := &thread.depths[thread.current_depth - 1]
					parent_ev := stack_peek_back(&thread.bande_q)

					pev := &parent_depth.events[parent_ev.idx]
				}
			} else {
				fmt.eprintf("Got unexpected end event! [pid: %d, tid: %d, ts: %v]\n", temp_ev.process_id, temp_ev.thread_id, temp_ev.timestamp)
			}
		}
	}

	// cleanup unfinished events
	for process in &trace.processes {
		for thread in &process.threads {
			for thread.bande_q.len > 0 {
				ev_data := stack_pop_back(&thread.bande_q)

				depth := &thread.depths[ev_data.depth]
				jev := &depth.events[ev_data.idx]

				thread.max_time = max(thread.max_time, jev.timestamp)
				trace.max_timestamp = max(trace.max_timestamp, jev.timestamp)
			}
		}
	}

	return trace
}

BinaryState :: enum {
	EventRead,
	Finished,
	Failure,
}

ms_v1_get_next_event :: proc(data: []u8, pos: ^int, trace: ^Trace, temp_ev: ^TempBinEvent) -> BinaryState {
	header_sz := size_of(u64)
	data_start := data[pos^:]
	type := (^spall.Event_Type)(raw_data(data_start))^
	#partial switch type {
	case .Begin:
		event_sz := size_of(spall.Begin_Event)
		event := (^spall.Begin_Event)(raw_data(data_start))

		event_tail := int(event.name_len) + int(event.args_len)
		name := string(data_start[event_sz:event_sz+int(event.name_len)])
		args := string(data_start[event_sz+int(event.name_len):event_sz+int(event.name_len)+int(event.args_len)])

		temp_ev.type = .Begin
		temp_ev.timestamp = i64(event.time)
		temp_ev.thread_id = event.tid
		temp_ev.process_id = event.pid
		temp_ev.name = intern_string(trace, name)
		temp_ev.args = intern_string(trace, args)

		pos^ += event_sz + event_tail
		return .EventRead
	case .End:
		event_sz := size_of(spall.End_Event)
		event := (^spall.End_Event)(raw_data(data_start))

		temp_ev.type = .End
		temp_ev.timestamp = i64(event.time)
		temp_ev.thread_id = event.tid
		temp_ev.process_id = event.pid
		
		pos^ += event_sz
		return .EventRead
	case:
		return .Failure
	}
}

ms_v1_bin_push_event :: proc(trace: ^Trace, process_id, thread_id: u32, event: ^Event) -> (uint, uint, uint) {
	p_idx := setup_pid(trace, process_id)
	p := &trace.processes[p_idx]
	p.min_time = min(p.min_time, event.timestamp)

	t_idx := setup_tid(p, thread_id)
	t := &p.threads[t_idx]
	t.min_time = min(t.min_time, event.timestamp)

	if t.max_time > event.timestamp {
		fmt.eprintf("Woah, time-travel? You just had a begin event that started before a previous one; [pid: %d, tid: %d, name: %s]\n", 
			process_id, thread_id, get_string(trace, event.name_offset))
		os.exit(1)
	}
	t.max_time = event.timestamp + event.duration

	trace.min_timestamp = min(trace.min_timestamp, event.timestamp)
	trace.max_timestamp = max(trace.max_timestamp, event.timestamp + event.duration)

	if int(t.current_depth) >= len(t.depths) {
		append(&t.depths, Depth{})
	}

	depth := &t.depths[t.current_depth]
	t.current_depth += 1
	append(&depth.events, event^)

	return p_idx, t_idx, len(depth.events)-1
}

setup_pid :: proc(trace: ^Trace, pid: u32) -> uint {
	if !(pid in trace.process_map) {
		trace.process_map[pid] = len(trace.processes)
		append(&trace.processes, Process{
			id = pid,
			min_time = max(i64),
		})
	}
	return trace.process_map[pid]
}

setup_tid :: proc(process: ^Process, tid: u32) -> uint {
	if !(tid in process.thread_map) {
		process.thread_map[tid] = len(process.threads)
		append(&process.threads, Thread{
			id = tid,
			min_time = max(i64),
			max_time = min(i64),
		})
	}
	return process.thread_map[tid]
}

intern_init :: proc(trace: ^Trace) {
	intern_string(trace, "")
}

intern_string :: proc(trace: ^Trace, str: string) -> u32 {
	if !(str in trace.string_offsets) {
		trace.string_offsets[str] = u32(len(trace.string_block))
		reserve(&trace.string_block, len(trace.string_block) + 2 + len(str))
		str_len := u16(len(str))
		str_len_bytes := (([^]u8)(&str_len)[:2])
		append(&trace.string_block, str_len_bytes[0])
		append(&trace.string_block, str_len_bytes[1])
		append_elem_string(&trace.string_block, str)
		if len(trace.string_block) >= (1 << 32) {
			fmt.eprintln("String block overflow")
			os.exit(1)
		}
	}
	return trace.string_offsets[str]
}

get_string :: proc(trace: ^Trace, idx: u32) -> string {
	length := int(trace.string_block[idx]) + 256 * int(trace.string_block[idx + 1])
	return string(trace.string_block[idx+2:idx+2+u32(length)])
}

make_directory_or_exit :: proc(path: string) {
	if os.make_directory(path) != 0 {
		fmt.eprintf("Couldn't make dir %v\n", path)
		os.exit(1)
	}
}

append_u8 :: proc(buf: ^[dynamic]u8, value: u8) {
	append(buf, value)
}

append_u32 :: proc(buf: ^[dynamic]u8, value: u32) {
	resize(buf, len(buf) + 4)
	endian.put_u32(buf[len(buf)-4:], .Little, value)
}

append_i32 :: proc(buf: ^[dynamic]u8, value: i32) {
	resize(buf, len(buf) + 4)
	endian.put_i32(buf[len(buf)-4:], .Little, value)
}

append_u64 :: proc(buf: ^[dynamic]u8, value: u64) {
	resize(buf, len(buf) + 8)
	endian.put_u64(buf[len(buf)-8:], .Little, value)
}

append_i64 :: proc(buf: ^[dynamic]u8, value: i64) {
	resize(buf, len(buf) + 8)
	endian.put_i64(buf[len(buf)-8:], .Little, value)
}

append_f32 :: proc(buf: ^[dynamic]u8, value: f32) {
	resize(buf, len(buf) + 4)
	endian.put_f32(buf[len(buf)-4:], .Little, value)
}

append_f64 :: proc(buf: ^[dynamic]u8, value: f64) {
	resize(buf, len(buf) + 8)
	endian.put_f64(buf[len(buf)-8:], .Little, value)
}