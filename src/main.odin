package main

import "core:fmt"
import "core:math"
import "core:math/rand"
import "core:runtime"
import "core:mem"
import "vendor:wasm/js"
import "core:container/queue"

// allocator state
temp_arena := Arena{}
debug_arena := Arena{}
scratch_arena := Arena{}
scratch2_arena := Arena{}
small_global_arena := Arena{}
big_global_arena := Arena{}

temp_allocator: mem.Allocator
debug_allocator: mem.Allocator
scratch_allocator: mem.Allocator
scratch2_allocator: mem.Allocator
small_global_allocator: mem.Allocator
big_global_allocator: mem.Allocator

wasmContext := runtime.default_context()

// input state
is_mouse_down  := false
was_mouse_down := false
clicked        := false
double_clicked := false
mouse_up_now   := false
is_hovering    := false
shift_down     := false
ctrl_down      := false

last_mouse_pos := Vec2{}
mouse_pos      := Vec2{}
clicked_pos    := Vec2{}

scroll_val_y:        f64 = 0
velocity_multiplier: f64 = 0
clicked_t:           f64 = 0

cam := Camera{Vec2{0, 0}, Vec2{0, 0}, 0, 1, 1}

// selection state
selected_func : u32 = 0
selected_event := empty_event
pressed_event  := empty_event
released_event := empty_event

clicked_on_rect := false

// tooltip-state
rect_tooltip_rect := empty_event
rect_tooltip_pos := Vec2{}
rendered_rect_tooltip := false

did_pan := false

stats_just_started := false
stats_state := StatState.NoStats
stat_sort_type := SortState.SelfTime
stat_sort_descending := true
resort_stats := false
cur_stat_offset := StatOffset{}
total_tracked_time: i64 = 0.0

// drawing state
colormode      := ColorMode.Dark

dpr          : f64 = 1
_p_font_size : f64 = 13
_h1_font_size: f64 = 17
_h2_font_size: f64 = 15

em            :f64 = _p_font_size
p_font_size   :f64 = 0
h1_font_size  :f64 = 0
h2_font_size  :f64 = 0
p_height      :f64 = 0
h1_height     :f64 = 0
h2_height     :f64 = 0

ch_width      :f64 = 0
min_ch_width  :f64 = 2
thread_gap    :f64 = 8

build_hash : i32 = 0
enable_debug := false
fps_history: queue.Queue(f64)

t               : f64
multiselect_t   : f64
greyanim_t      : f32
greymotion      : f32
anim_playing    : bool
frame_count     : int
last_frame_count: int
rect_count      : int
bucket_count    : int
was_sleeping    : bool
random_seed     : u64
lfc : int
first_frame := true

// loading / trace state
_trace: Trace
loading_config := false
post_loading := true
last_read: i64

CHUNK_SIZE :: 10 * 1024 * 1024

ui_state: UIState
gl_rects: [dynamic]DrawRect
awake := true

set_flamegraph_camera :: proc(trace: ^Trace, ui_state: ^UIState, start_ticks, duration_ticks: i64) {
	cam.vel = Vec2{}

	cam.current_scale = rescale(1.0, 0, f64(duration_ticks), 0, ui_state.full_flamegraph_rect.w)
	cam.target_scale = cam.current_scale

	adj_start_ticks := f64(start_ticks - trace.total_min_time)

	cam.pan.x = -(adj_start_ticks * cam.current_scale)
	cam.target_pan_x = cam.pan.x
}

reset_flamegraph_camera :: proc(trace: ^Trace, ui_state: ^UIState) {
	cam = Camera{Vec2{0, 0}, Vec2{0, 0}, 0, 1, 1}

	start_time: f64 = 0
	end_time  := f64(trace.total_max_time - trace.total_min_time)

	side_pad  := 2 * em

	cam.current_scale = rescale(cam.current_scale, start_time, end_time, 0, ui_state.full_flamegraph_rect.w - (side_pad * 2))
	cam.target_scale = cam.current_scale

	cam.pan.x += side_pad
	cam.target_pan_x = cam.pan.x
}

// Most of the action happens in frame(), this is just to set up for the JS/WASM platform layer
main :: proc() {
	ONE_GB_PAGES :: 1 * 1024 * 1024 * 1024 / js.PAGE_SIZE
	ONE_MB_PAGES :: 1 * 1024 * 1024 / js.PAGE_SIZE
	temp_data, _         := js.page_alloc(ONE_MB_PAGES * 75)
	debug_data, _        := js.page_alloc(1)
	scratch_data, _      := js.page_alloc(ONE_MB_PAGES * 20)
	scratch2_data, _     := js.page_alloc(ONE_MB_PAGES * 50)
	small_global_data, _ := js.page_alloc(ONE_MB_PAGES * 1)

	arena_init(&temp_arena,         temp_data)
	arena_init(&debug_arena,        debug_data)
	arena_init(&scratch_arena,      scratch_data)
	arena_init(&scratch2_arena,     scratch2_data)
	arena_init(&small_global_arena, small_global_data)

	// This must be init last, because it grows infinitely.
	// We don't want it accidentally growing into anything useful.
	growing_arena_init(&big_global_arena)

	// I'm doing olympic-level memory juggling BS in the ingest system because
	// arenas are *special*, and memory is *precious*. Beware free_all()'ing
	// the wrong one at the wrong time, here thar be dragons. Once you're in
	// normal render/frame space, I free_all temp once per frame, and I shouldn't
	// need to touch scratch
	temp_allocator         = arena_allocator(&temp_arena)
	debug_allocator        = arena_allocator(&debug_arena)
	scratch_allocator      = arena_allocator(&scratch_arena)
	scratch2_allocator     = arena_allocator(&scratch2_arena)
	small_global_allocator = arena_allocator(&small_global_arena)

	big_global_allocator = growing_arena_allocator(&big_global_arena)

	wasmContext.allocator      = big_global_allocator
	wasmContext.temp_allocator = temp_allocator

	context = wasmContext

	// fibhashing the time to get better seed distribution
	random_seed = u64(get_time()) * 11400714819323198485
	rand.set_global_seed(random_seed)

	queue.init(&fps_history, 0, debug_allocator)

	_trace = Trace{}
	ui_state = UIState{}
	next_line(&ui_state.line_height, em)
	// ui_state.info_pane_height = ui_state.line_height * 8
}

@export
frame :: proc "contextless" (width, height: f64, _dt: f64, current_time: f64) -> bool {
	context = wasmContext
	defer {
		clicked = false
		double_clicked = false
		is_hovering = false
		was_mouse_down = false
		mouse_up_now = false
		released_event = {-1, -1, -1, -1}

		ui_state.render_one_more = false
		frame_count += 1
		lfc += 1
		free_all(context.temp_allocator)
	}

	rect_tooltip_rect = empty_event
	rect_tooltip_pos = Vec2{}
	rendered_rect_tooltip = false

	if first_frame {
		clicked_t = current_time
		manual_load(default_config, default_config_name)
		first_frame = false
		return true
	}

	// render loading screen
	if loading_config {
		pad_size : f64 = 4
		chunk_size : f64 = 10

		load_box := Rect{0, 0, 100, 100}
		load_box = Rect{
			(width / 2) - (load_box.w / 2) - pad_size, 
			(height / 2) - (load_box.h / 2) - pad_size, 
			load_box.w + pad_size, 
			load_box.h + pad_size,
		}

		draw_rect(load_box, BVec4{30, 30, 30, 255})
		chunk_count := int(rescale(f64(_trace.parser.offset), 0, f64(_trace.parser.total_size), 0, 100))

		chunk := Rect{0, 0, chunk_size, chunk_size}
		start_x := load_box.x + pad_size
		start_y := load_box.y + pad_size
		for i := chunk_count; i >= 0; i -= 1 {
			cur_x := f64(i %% int(chunk_size))
			cur_y := f64(i /  int(chunk_size))
			draw_rect(Rect{
				start_x + (cur_x * chunk_size), 
				start_y + (cur_y * chunk_size), 
				chunk_size - pad_size, 
				chunk_size - pad_size,
			}, loading_block_color)
		}

		return true
	}


	dt := _dt

	if queue.len(fps_history) > 100 { queue.pop_front(&fps_history) }
	queue.push_back(&fps_history, 1 / dt)

	if was_sleeping {
		dt = 0.001
		was_sleeping = false
	}
	t = current_time

	if !event_cmp(_trace.zoom_event, empty_event) {
		ev := get_event(&_trace, _trace.zoom_event)
		thread := _trace.processes[_trace.zoom_event.pid].threads[_trace.zoom_event.tid]
		duration := bound_duration(ev, thread.max_time)

		set_flamegraph_camera(&_trace, &ui_state, ev.timestamp, duration)
		_trace.zoom_event = empty_event
	}

	// update animation timers
	greyanim_t = f32((t - multiselect_t) * 5)
	greymotion = ease_in_out(greyanim_t)

	// Set up all the display state we need to render the screen
	update_font_cache()

	ui_state.height = height
	ui_state.width  = width
	spall_x_pad     := 3 * em
	header_height   := 3 * em
	activity_height := 2 * em
	timebar_height  := 2.25 * em
	rect_height     := em + (0.75 * em)
	top_line_gap    := (em / 1.5)

	topbars_height    := header_height + timebar_height + activity_height
	minigraph_width   := 15 * em
	flamegraph_width  := ui_state.width - (spall_x_pad + minigraph_width)
	flamegraph_height := ui_state.height - topbars_height - ui_state.info_pane_height

	tab_select_height := 2 * em
	filter_pane_width := ui_state.filters_open ? (15 * em) : 0
	stats_pane_x := filter_pane_width

	ui_state.side_pad                  = spall_x_pad
	ui_state.rect_height               = rect_height
	ui_state.topbars_height            = topbars_height
	ui_state.top_line_gap              = top_line_gap
	ui_state.flamegraph_toptext_height = (ui_state.top_line_gap * 2) + (2 * em)
	ui_state.flamegraph_header_height  = ui_state.flamegraph_toptext_height + em

	ui_state.header_rect             = Rect{0, 0, ui_state.width, header_height}
	ui_state.global_timebar_rect     = Rect{0, header_height, ui_state.width, timebar_height}
	ui_state.global_activity_rect    = Rect{spall_x_pad, header_height + timebar_height, flamegraph_width, activity_height}
	ui_state.local_timebar_rect      = Rect{spall_x_pad, header_height + timebar_height + activity_height, flamegraph_width, timebar_height}
	ui_state.minimap_rect            = Rect{width - minigraph_width, topbars_height, minigraph_width, flamegraph_height}

	ui_state.info_pane_rect          = Rect{0, ui_state.height - ui_state.info_pane_height, ui_state.width, ui_state.info_pane_height}
	ui_state.tab_rect                = Rect{0, ui_state.info_pane_rect.y, ui_state.width, tab_select_height}

	pane_start_y := ui_state.tab_rect.y + ui_state.tab_rect.h

	info_subpane_height := ui_state.info_pane_height - tab_select_height
	ui_state.filter_pane_rect        = Rect{0, pane_start_y, filter_pane_width, info_subpane_height}
	ui_state.stats_pane_rect         = Rect{stats_pane_x, pane_start_y, ui_state.width - stats_pane_x, info_subpane_height}

	ui_state.full_flamegraph_rect    = Rect{spall_x_pad, topbars_height, flamegraph_width, flamegraph_height}

	ui_state.inner_flamegraph_rect    = ui_state.full_flamegraph_rect
	ui_state.inner_flamegraph_rect.y += ui_state.flamegraph_toptext_height
	ui_state.inner_flamegraph_rect.h -= ui_state.flamegraph_toptext_height

	ui_state.padded_flamegraph_rect    = ui_state.inner_flamegraph_rect
	ui_state.padded_flamegraph_rect.y += em
	ui_state.padded_flamegraph_rect.h -= em

	if post_loading {
		reset_flamegraph_camera(&_trace, &ui_state)
		ui_state.multiselecting = false
		lfc = 0
		post_loading = false
	}

	// process key/mouse inputs

	if clicked {
		did_pan = false
		pressed_event = {-1, -1, -1, -1} // so no stale events are tracked
	}
	start_time, end_time, pan_delta := process_inputs(&_trace, dt, &ui_state)

	clicked_on_rect = false
	rect_count = 0
	bucket_count = 0

	// Init GL / Text canvases
	canvas_clear()
	gl_init_frame(bg_color2)
	gl_rects = make([dynamic]DrawRect, 0, int(width / 2), temp_allocator)

	graph_visible_range := draw_flamegraphs(&_trace, start_time, end_time, &ui_state)

	minimap_visible_range := draw_minimap(&_trace, &ui_state)
	global_activity_visible_range := draw_topbars(&_trace, start_time, end_time, &ui_state)

	tilemap_fetch_missing(&_trace, graph_visible_range, minimap_visible_range, global_activity_visible_range)

	// draw sidelines
	draw_line(Vec2{ui_state.side_pad, header_height + timebar_height},       Vec2{ui_state.side_pad, ui_state.info_pane_rect.y}, 1, line_color)
	draw_line(Vec2{ui_state.minimap_rect.x, header_height + timebar_height}, Vec2{ui_state.minimap_rect.x, ui_state.info_pane_rect.y}, 1, line_color)

	// process_multiselect(&_trace, pan_delta, dt, &ui_state)
	// process_stats(&_trace, &ui_state)

	// draw_stats(&_trace, &ui_state)
	// stats_just_started = false
	// if resort_stats {
	// 	sort_stats(&_trace)
	// 	resort_stats = false
	// }

	draw_header(&_trace, &ui_state)

	// reset the cursor if we're not over a selectable thing
	if !is_hovering {
		reset_cursor()
	}

	if enable_debug {
		draw_debug(&ui_state)
	}

	// if there's a rectangle tooltip to render, now's the time.
	if rendered_rect_tooltip {
		draw_rect_tooltip(&_trace, &ui_state)
	}

	if _trace.error_message != "" {
		draw_errorbox(&_trace, &ui_state)
	}

	// save me my battery, plz
	if should_sleep(&cam, &ui_state) {
		cam.pan.x = cam.target_pan_x
		cam.vel.y = 0
		cam.current_scale = cam.target_scale
		ui_state.stats_pane_scroll_vel = 0
		ui_state.filter_pane_scroll_vel = 0

		was_sleeping = true
		return false
	} else {
		was_sleeping = false
		return true
	}
}

should_sleep :: proc(cam: ^Camera, ui_state: ^UIState) -> bool {
	PAN_X_EPSILON :: 0.01
	PAN_Y_EPSILON :: 1.0
	SCALE_EPSILON :: 0.01
	SCROLL_EPSILON :: 0.01

	panning_x := math.abs(cam.pan.x - cam.target_pan_x) > PAN_X_EPSILON
	panning_y := math.abs(cam.vel.y - 0) > PAN_Y_EPSILON
	scaling   := math.abs((cam.current_scale - cam.target_scale) / cam.target_scale) > SCALE_EPSILON
	scrolling := (math.abs(ui_state.filter_pane_scroll_vel) > SCROLL_EPSILON) || (math.abs(ui_state.stats_pane_scroll_vel) > SCROLL_EPSILON)

	return (!ui_state.render_one_more && !panning_x && !panning_y && !scaling && !scrolling)
}
