package main

import "core:fmt"
import "core:hash"
import "core:runtime"
import "core:strings"

// u32 -> u32 map
PTEntry :: struct {
	key: u32,
	val: int,
}
ValHash :: struct {
	entries: [dynamic]PTEntry,
	hashes:  [dynamic]int,
	len_minus_one: u32,
}

vh_init :: proc(allocator := context.allocator) -> ValHash {
	v := ValHash{}
	v.entries = make([dynamic]PTEntry, 0, allocator)
	v.hashes = make([dynamic]int, 32, allocator) // must be a power of two
	for i in 0..<len(v.hashes) {
		v.hashes[i] = -1
	}
	v.len_minus_one = u32(len(v.hashes) - 1)
	return v
}

// this is a fibhash.. Replace me if I'm dumb
vh_hash :: proc "contextless" (key: u32) -> u32 {
	return key * 2654435769
}

vh_find :: proc "contextless" (v: ^ValHash, key: u32, loc := #caller_location) -> (int, bool) {
	hv := vh_hash(key) & v.len_minus_one

	for i: u32 = 0; i < u32(len(v.hashes)); i += 1 {
		idx := (hv + i) & v.len_minus_one

		e_idx := v.hashes[idx]
		if e_idx == -1 {
			return -1, false
		}

		if v.entries[e_idx].key == key {
			return v.entries[e_idx].val, true
		}
	}

	return -1, false
}

vh_grow :: proc(v: ^ValHash) {
	resize(&v.hashes, len(v.hashes) * 2)
	for i in 0..<len(v.hashes) {
		v.hashes[i] = -1
	}
	v.len_minus_one = u32(len(v.hashes) - 1)

	for entry, idx in v.entries {
		vh_reinsert(v, entry, idx)
	}
}

vh_reinsert :: proc "contextless" (v: ^ValHash, entry: PTEntry, v_idx: int) {
	hv := vh_hash(entry.key) & v.len_minus_one
	for i: u32 = 0; i < u32(len(v.hashes)); i += 1 {
		idx := (hv + i) & v.len_minus_one

		e_idx := v.hashes[idx]
		if e_idx == -1 {
			v.hashes[idx] = v_idx
			return
		}
	}
}

vh_insert :: proc(v: ^ValHash, key: u32, val: int) {
	if len(v.entries) >= int(f64(len(v.hashes)) * 0.75) {
		vh_grow(v)
	}

	hv := vh_hash(key) & v.len_minus_one
	for i: u32 = 0; i < u32(len(v.hashes)); i += 1 {
		idx := (hv + i) & v.len_minus_one

		e_idx := v.hashes[idx]
		if e_idx == -1 {
			v.hashes[idx] = len(v.entries)
			append(&v.entries, PTEntry{key, val})
			return
		} else if v.entries[e_idx].key == key {
			v.entries[e_idx] = PTEntry{key, val}
			return
		}
	}

	push_fatal(SpallError.Bug)
}

INMAP_LOAD_FACTOR :: 0.75

INStr :: struct #packed {
	start: u32,
	len: u16,
}

// String interning
INMap :: struct {
	entries: [dynamic]INStr,
	hashes:  [dynamic]int,
	resize_threshold: i64,
	len_minus_one: u32,
}

in_init :: proc(allocator := context.allocator) -> INMap {
	v := INMap{}
	v.entries = make([dynamic]INStr, 0, allocator)
	v.hashes = make([dynamic]int, 32, allocator) // must be a power of two
	for i in 0..<len(v.hashes) {
		v.hashes[i] = -1
	}
	v.resize_threshold = i64(f64(len(v.hashes)) * INMAP_LOAD_FACTOR) 
	v.len_minus_one = u32(len(v.hashes) - 1)
	return v
}

in_hash :: proc (key: string) -> u32 {
	k := transmute([]u8)key
	return #force_inline hash.murmur32(k)
}


in_reinsert :: proc (v: ^INMap, entry: INStr, v_idx: int) {
	hv := in_hash(in_getstr(entry)) & v.len_minus_one
	for i: u32 = 0; i < u32(len(v.hashes)); i += 1 {
		idx := (hv + i) & v.len_minus_one

		e_idx := v.hashes[idx]
		if e_idx == -1 {
			v.hashes[idx] = v_idx
			return
		}
	}
}

in_grow :: proc(v: ^INMap) {
	resize(&v.hashes, len(v.hashes) * 2)
	for i in 0..<len(v.hashes) {
		v.hashes[i] = -1
	}

	v.resize_threshold = i64(f64(len(v.hashes)) * INMAP_LOAD_FACTOR) 
	v.len_minus_one = u32(len(v.hashes) - 1)
	for entry, idx in v.entries {
		in_reinsert(v, entry, idx)
	}
}

in_get :: proc(v: ^INMap, key: string) -> INStr {
	if i64(len(v.entries)) >= v.resize_threshold {
		in_grow(v)
	}

	hv := in_hash(key) & v.len_minus_one
	for i: u32 = 0; i < u32(len(v.hashes)); i += 1 {
		idx := (hv + i) & v.len_minus_one

		e_idx := v.hashes[idx]
		if e_idx == -1 {
			v.hashes[idx] = len(v.entries)

			str_start := u32(len(string_block))
			in_str := INStr{str_start, u16(len(key))}
			append_elem_string(&string_block, key)
			append(&v.entries, in_str)

			return in_str
		} else if in_getstr(v.entries[e_idx]) == key {
			return v.entries[e_idx]
		}
	}

	push_fatal(SpallError.Bug)
}

in_getstr :: #force_inline proc(v: INStr) -> string {
	return string(string_block[v.start:v.start+u32(v.len)])
}

KM_CAP :: 32

// Key mashing
KeyMap :: struct {
	keys:   [KM_CAP]string,
	types: [KM_CAP]FieldType,
	hashes: [KM_CAP]int,
	len: int,
}

km_init :: proc() -> KeyMap {
	v := KeyMap{}
	for i in 0..<len(v.hashes) {
		v.hashes[i] = -1
	}
	return v
}

// lol, fibhash win
km_hash :: proc "contextless" (key: string) -> u32 #no_bounds_check {
	return u32(key[0]) * 2654435769 
}

// expects that we only get static strings
km_insert :: proc(v: ^KeyMap, key: string, type: FieldType) #no_bounds_check {
	hv := km_hash(key) & (KM_CAP - 1)
	for i: u32 = 0; i < KM_CAP; i += 1 {
		idx := (hv + i) & (KM_CAP - 1)

		e_idx := v.hashes[idx]
		if e_idx == -1 {
			v.hashes[idx] = v.len
			v.keys[v.len] = key
			v.types[v.len] = type
			v.len += 1
			return
		} else if v.keys[e_idx] == key {
			return
		}
	}

	push_fatal(SpallError.Bug)
}

km_find :: proc (v: ^KeyMap, key: string) -> (FieldType, bool) #no_bounds_check {
	hv := km_hash(key) & (KM_CAP - 1)

	for i: u32 = 0; i < KM_CAP; i += 1 {
		idx := (hv + i) & (KM_CAP - 1)

		e_idx := v.hashes[idx]
		if e_idx == -1 {
			return .Invalid, false
		}

		if v.keys[e_idx] == key {
			return v.types[e_idx], true
		}
	}

	return .Invalid, false
}
