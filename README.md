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

### Run Hash Table Resizing
```
server:
sudo ./build/benchs/outback/server --nic_idx=2 --seconds=120 --nkeys=2000000 --mem_threads=1 --workloads=ycsbd
```
``` 
client: 
sudo ./build/benchs/outback/client --nic_idx=2 --server_addr=192.168.1.2:8888 --seconds=120 --coros=1 --nkeys=2000000 --bench_nkeys=100000 --mem_threads=1 --threads=8 --workloads=ycsbd

Note that if you use r320, the ```nic_idx``` should be set as 0, also parameter ```mem_threads``` should be the same in both client and server. 

workloads insert

nix_idx
