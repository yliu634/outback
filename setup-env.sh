#!/bin/bash

mode="$1"
ubuntu_version=$(lsb_release -r -s)

if [ $ubuntu_version == "18.04" ]; then
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz ofed.tgz
elif [ $ubuntu_version == "20.04" ]; then
  # wget https://content.mellanox.com/ofed/MLNX_OFED-23.10-3.2.2.0/MLNX_OFED_LINUX-23.10-3.2.2.0-ubuntu20.04-x86_64.tgz
  # wget https://content.mellanox.com/ofed/MLNX_OFED-5.8-5.1.1.2/MLNX_OFED_LINUX-5.8-5.1.1.2-ubuntu20.04-x86_64.tgz
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz ofed.tgz
else
  echo "Wrong ubuntu distribution for $mode!"
  exit 0
fi
echo $mode $ubuntu_version $ofed_fid

sudo apt update -y
sudo apt -y install g++ cmake clang python-pip sysstat zstd libtbb-dev libgtest-dev libboost-all-dev google-perftools libgoogle-perftools-dev cmake build-essential pkgconf gdb libssl-dev tmux liblua5.3-dev libgflags-dev libmemcached-dev libmemcached-tools memcached libnuma-dev linux-tools-common linux-tools-$(uname -r)

# install anaconda
mkdir install
mv ofed.tgz install
pip install gdown

# install ofed
cd install
if [ ! -d "./ofed" ]; then
  tar zxf ofed.tgz
  mv MLNX* ofed
fi
cd ofed
sudo ./mlnxofedinstall --force
sudo /etc/init.d/openibd restart
sudo /etc/init.d/opensmd restart
cd ..

# install cmake
cd install
if [ ! -f cmake-3.16.8.tar.gz ]; then
  wget https://cmake.org/files/v3.16/cmake-3.16.8.tar.gz
fi
if [ ! -d "./cmake-3.16.8" ]; then
  tar zxf cmake-3.16.8.tar.gz
  cd cmake-3.16.8 && ./configure && make -j 4 && sudo make install
fi
cd ..

# install abseil-cpp
cd install
if [ ! -f abseil-cpp ]; then
  git clone https://github.com/abseil/abseil-cpp.git
fi
cd abseil-cpp && mkdir build && cd build && cmake .. && make -j  && sudo make install
find ./ -name "*.o" | xargs ar cr libabsl.a 
sudo cp libabsl.a /usr/lib
cd ../..

# install gtest
if [ ! -d "/usr/src/gtest" ]; then
  sudo apt install -y libgtest-dev
fi
cd /usr/src/gtest
sudo cmake .
sudo make
sudo cp /usr/src/gtest/lib/libgtest*.a /usr/local/lib/
sudo cp -r /usr/src/gtest/include/gtest /usr/local/include/

# config huge page
sudo sh -c 'echo 1400 > /proc/sys/vm/nr_hugepages'
cat /proc/meminfo | grep Huge

# kill -9 $(lsof -ti :18515)

