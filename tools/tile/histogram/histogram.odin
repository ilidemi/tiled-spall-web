package historgram

import "core:fmt"
import "core:os"

main :: proc() {
	if !(len(os.args) == 2) {
		fmt.eprintf("%v <folder>\n", os.args[0])
		os.exit(1)
	}

	folder_name := os.args[1]
	buckets: [dynamic]BucketCount
	append(&buckets, BucketCount {
		bucket_size = 8,
	})
	make_histogram(folder_name, &buckets)

	total_count := 0
	for bucket in buckets {
		total_count += bucket.count
	}
	for bucket in buckets {
		fmt.printf("%d: %.03f%%\n", bucket.bucket_size, f64(bucket.count) * 100 / f64(total_count))
	}
}

BucketCount :: struct {
	bucket_size: i64,
	count: int,
}

make_histogram :: proc(path: string, buckets: ^[dynamic]BucketCount) {
	handle, err := os.open(path)
	if err != 0 {
		fmt.eprintf("open %s error: %v\n", path, err)
	}
	defer os.close(handle)
	file_infos, err2 := os.read_dir(handle, 0)
	if err2 != 0 {
		fmt.eprintf("readdir %s error: %v\n", path, err)
		os.exit(1)
	}

	for file_info in file_infos {
		if file_info.is_dir {
			dir_path := fmt.tprintf("%s/%s", path, file_info.name)
			make_histogram(dir_path, buckets)
		} else {
			for file_info.size > buckets[len(buckets) - 1].bucket_size {
				append(buckets, BucketCount{
					bucket_size = buckets[len(buckets) - 1].bucket_size * 2,
				})
			}
			for _, i in buckets {
				if file_info.size <= buckets[i].bucket_size {
					buckets[i].count += 1
					break
				}
			}
		}
	}
}