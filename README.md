## OUTBACK
built on top of Rolex/XStore/R2..
```
sudo ./benchs/outback/client --threads=3 --mem_threads=3 --server_addr=192.168.1.1:8888 
sudo ./benchs/outback/server --mem_threads=1
```
### Build
1. Dependency
```
MLNX_OFED_LINUX-5.4-1.0.3.0-ubuntu18.04-x86_64
Abseil-cpp master
Boost_1.65
Gflags_2.2.2
```
2. CMake
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```
3. Create HugePage
### Run
```
sudo su
echo 1400 > /proc/sys/vm/nr_hugepages
exit 

sudo ifconfig ibp8s0 192.168.1.1 netmask 255.255.0.0
sudo ifconfig ibp8s0 192.168.1.2 netmask 255.255.0.0
./rolex --nkeys=100000 --non_nkeys=100000
```
   
| parameters | descriptions |
|  ----  | ----  |
| --nkeys  | the number of trained data |
| --non_nkeys  | the number of new data |
| --threads  | the number of threads |
| --coros  | the number of coroutines |
| --workloads  | workloads |
| --read_ratio  | the read ratio |
| --insert_ratio  | the write ratio |
| --update_ratio  | the update ratio |.
