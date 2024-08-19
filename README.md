## OUTBACK
built on top of Rolex/XStore/R2..

Fast and Communication-efficient Index for Key-Value Store on Disaggregated Memory.

### Build
```
./outback/setup_env.sh
cd outback
./build.sh
```

### Test RNIC
* CloudLab r650 w./ Mlnx CX6 100 Gb NIC (~92.57Gbits):
    ```
    server:
    sudo ifconfig ens2f0 192.168.1.2 netmask 255.255.0.0
    ib_write_bw -d mlx5_2 -i 1 -D 10 --report_gbits
    ```
    ```
    client:
    sudo ifconfig ens2f0 192.168.1.0 netmask 255.255.0.0
    ib_write_bw 192.168.1.2 -d mlx5_2 -i 1 -D 10 --report_gbits
    ```
* Cloudlab r320 w./ Mlnx MX354A FDR CX3 adapter (~55.52Gbits):
    ```
    server:
    sudo ifconfig ibp8s0 192.168.1.2 netmask 255.255.0.0
    ib_write_bw --report_gbits
    ```
    ```
    client:
    sudo ifconfig ibp8s0 192.168.1.0 netmask 255.255.0.0
    ib_write_bw 192.168.1.2 -d mlx4_0 -i 1 -D 10 --report_gbits
    ```

### Run throughput benchmark
```
server:
sudo taskset -c 0 ./build/benchs/outback/server --seconds=120 --nkeys=50000000 --mem_threads=1 --workloads=ycsbc
```
``` 
client:
sudo taskset -c 0-71 ./build/benchs/outback/client --nic_idx=2 --server_addr=192.168.1.2:8888 --seconds=120 --nkeys=50000000 --bench_nkeys=10000000 --coros=2 --mem_threads=1 --threads=72 --workloads=ycsbc
```
Note that if you use r320, the ```--nic_idx``` should be set as 0, also parameter ```--mem_threads``` should be the same in both client and server, ```numactl --physcpubind=0-71``` may also works. 
