find profile_big.spall.tiles.br -type f -name *.spalltile | parallel --jobs 12 -n 10 --shuf --bar brotli -1 -j -f