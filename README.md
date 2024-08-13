## OUTBACK
built on top of Rolex/XStore/R2..

### Build
1. Dependency
```
MLNX_OFED_LINUX-5.4-1.0.3.0-ubuntu18.04-x86_64
Abseil-cpp master
Boost_1.65
Gflags_2.2.2
```
2. Create HugePage
```
$ echo 
```
3. CMake
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

### Run
```
sudo ./benchs/outback/server --nkeys=100000 --mem_threads=2
sudo ./benchs/outback/client --server_addr=192.168.1.2:8888 --nkeys=100000 --mem_threads=2 --threads=16
```

