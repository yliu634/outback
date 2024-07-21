#pragma once

#include <random>
#include <vector>
#include <cassert>

#include "lib.hh" 
#include "rolex_util_back.hh" 
#include "load_config.hh"  
#include "ycsb/core_workload.h"

namespace bench {
//using namespace xstore;

std::vector<u64> exist_keys;
std::vector<u64> nonexist_keys;
std::vector<u64> bench_keys;

ycsbc::CoreWorkload workload;

// ========= functions ==============
void load_data();
void normal_data();
void normal_data_back();
void lognormal_data();
void load_ycsb();
void book_data();
void osm_data();
void wiki_ts_data();
void fb_data();


void load_data() {
	switch(BenConfig.workloads) {
		case NORMAL:
			normal_data_back(); // change this to not debug
      LOG(3) << "==== LOAD normal =====";
			break;
    case LOGNORMAL:
			lognormal_data();
      LOG(3) << "==== LOAD lognormal =====";
			break;
    case BOOK:
			book_data();
      LOG(3) << "==== LOAD books =====";
			break;
    case OSM:
			osm_data();
      LOG(3) << "==== LOAD osm cellids =====";
			break;
    case WIKI:
      wiki_ts_data();
      LOG(3) << "==== LOAD wikipedia =====";
			break;
    case FB:
      fb_data();
      LOG(3) << "==== LOAD facebook =====";
			break;
    case YCSB_A:
      BenConfig.read_ratio=0.5;
      BenConfig.update_ratio=0.5;
      BenConfig.non_nkeys=1;
      load_ycsb();
      LOG(3) << "==== LOAD YCSB A workload =====";
      break;
    case YCSB_B:
      BenConfig.read_ratio=0.95;
      BenConfig.update_ratio=0.05;
      BenConfig.non_nkeys=1;
      load_ycsb();
      LOG(3) << "==== LOAD YCSB B workload =====";
      break;
    case YCSB_C:
      BenConfig.read_ratio=1.0;
      BenConfig.non_nkeys=1;
      load_ycsb();
      LOG(3) << "==== LOAD YCSB C workload =====";
      break;
    case YCSB_D:
      BenConfig.read_ratio=0.95;
      BenConfig.insert_ratio=0.05;
      BenConfig.dists="latest";
      BenConfig.non_nkeys=BenConfig.nkeys/10;
      load_ycsb();
      LOG(3) << "==== LOAD YCSB D workload =====";
      break;
    case YCSB_E:
      LOG(3) << "YCSB E (Scans) not supported in Outback";
      exit(0);
      // BenConfig.scan_ratio=0.95;
      // BenConfig.insert_ratio=0.05;
      load_ycsb();
      LOG(3) << "==== LOAD YCSB E workload =====";
      break;
    case YCSB_F:
      BenConfig.read_ratio=0.5;
      BenConfig.insert_ratio=0.25;
      BenConfig.update_ratio=0.25;
      BenConfig.non_nkeys=BenConfig.nkeys/8;
      load_ycsb();
      LOG(3) << "==== LOAD YCSB F workload =====";
      break;
		default:
			LOG(4) << "WRONG benchmark " << BenConfig.workloads;
			exit(0);
	}
  LOG(3) << "Exist keys: " << exist_keys.size();
  LOG(3) << "nonExist keys: " << nonexist_keys.size();
}

/**
 * @brief generate normal data
 * 		benchmark 0: normal data
 */
void normal_data_back() {
  exist_keys.reserve(BenConfig.nkeys);
  for(size_t i=0; i<BenConfig.nkeys; i++) {
    u64 a = i;
    exist_keys.push_back(a);
  }
  nonexist_keys.reserve(BenConfig.non_nkeys);
  for(size_t i=0; i<BenConfig.non_nkeys; i++) {
    u64 a = BenConfig.nkeys+i;
    nonexist_keys.push_back(a);
  }
  bench_keys.reserve(BenConfig.bench_nkeys);
  for (size_t i=0; i<BenConfig.bench_nkeys; i++) {
    u64 a = i;
    bench_keys.push_back(a);
  }
}

void normal_data() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::normal_distribution<double> rand_normal(4, 2);
  
  exist_keys.reserve(BenConfig.nkeys);
  for(size_t i=0; i<BenConfig.nkeys; i++) {
    u64 a = rand_normal(gen)*1000000000000;
    if(a<0) {
      i--;
      continue;
    }
    exist_keys.push_back(a);
  }
  nonexist_keys.reserve(BenConfig.non_nkeys);
  for(size_t i=0; i<BenConfig.non_nkeys; i++) {
    u64 a = rand_normal(gen)*1000000000000;
    if(a<0) {
      i--;
      continue;
    }
    nonexist_keys.push_back(a);
  }
  bench_keys.reserve(BenConfig.bench_nkeys);
  for (size_t i=0; i<BenConfig.bench_nkeys; i++) {
    bench_keys.push_back(exist_keys[rand()%BenConfig.nkeys]);
  }
}

void lognormal_data(){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::lognormal_distribution<double> rand_lognormal(0, 2);

    exist_keys.reserve(BenConfig.nkeys);
    for (size_t i = 0; i < BenConfig.nkeys; ++i) {
      u64 a = rand_lognormal(gen)*1000000000000;
      assert(a>0);
      exist_keys.push_back(a);
    }
    if (BenConfig.insert_ratio > 0) {
      nonexist_keys.reserve(BenConfig.non_nkeys);
      for (size_t i = 0; i < BenConfig.non_nkeys; ++i) {
        u64 a = rand_lognormal(gen)*1000000000000;
        assert(a>0);
        nonexist_keys.push_back(a);
      }
    }
    bench_keys.reserve(BenConfig.bench_nkeys);
    for (size_t i=0; i<BenConfig.bench_nkeys; i++) {
      bench_keys.push_back(exist_keys[rand()%BenConfig.nkeys]);
    }
}

void load_ycsb() {
  exist_keys.reserve(BenConfig.nkeys);
  nonexist_keys.reserve(BenConfig.non_nkeys);
  size_t item_num = 0;
  workload.Init(BenConfig.nkeys, 
                BenConfig.bench_nkeys,
                BenConfig.dists,
                BenConfig.zip_const);   //uniform zipfian latest
  while(item_num < BenConfig.nkeys) { 
    std::string key_char = workload.NextSequenceKey();
    KeyType dummy_key = std::stoull(key_char.substr(4));
    exist_keys.push_back(dummy_key);
    item_num ++;
  }
  item_num = 0;
  bench_keys.reserve(BenConfig.bench_nkeys);
  while (item_num < BenConfig.bench_nkeys) {
    std::string key_char = workload.NextTransactionKey();
    KeyType dummy_key = std::stoull(key_char.substr(4), nullptr, 10);
    bench_keys.push_back(dummy_key);
    item_num ++;
  }
  item_num = 0;
  while (item_num < BenConfig.non_nkeys) {
    std::string key_char = workload.NextSequenceKey();
    KeyType dummy_key = std::stoull(key_char.substr(4));
    nonexist_keys.push_back(dummy_key);
    item_num ++;
  }
  LOG(3)<<"load ycsb exist_keys number : " << exist_keys.size();
  LOG(3)<<"load ycsb non_exist_keys number : " << nonexist_keys.size();
  LOG(3)<<"load bench_keys number : " << bench_keys.size();
}


#define BUF_SIZE 1
void read_file_data(std::string path) {
  std::vector<uint64_t> vec;
  FILE *fin = fopen(path.c_str(), "rb");
  uint64_t item_num(0);
  int64_t buf[BUF_SIZE];
  fseek(fin,sizeof(uint64_t),SEEK_SET); //skip the two number:200M
  while (item_num < BenConfig.nkeys) {
    size_t num_read = fread(buf, sizeof(uint64_t), BUF_SIZE, fin);
    for (size_t i = 0; i < num_read; i++) {
      vec.push_back(buf[i]);
      //LOG(3) << "exist_key: " << buf[i];
      item_num += num_read;
    }
    if (num_read < BUF_SIZE) break;
  }
  exist_keys = std::move(vec);
  LOG(3) << "Load exist_keys size: " << exist_keys.size();

  item_num = 0;
  while (item_num < BenConfig.non_nkeys) {
    size_t num_read = fread(buf, sizeof(uint64_t), BUF_SIZE, fin);
    for (size_t i = 0; i < num_read; i++) {
      nonexist_keys.push_back(buf[i]);
      //LOG(3) << "nonexist_key: " << buf[i];
      item_num += num_read;
    }
    if (num_read < BUF_SIZE) break;
  }
  fclose(fin);
  LOG(3) << "Load non_exist_keys size: " << nonexist_keys.size();

  item_num = 0;
  size_t m = exist_keys.size();
  while (item_num < BenConfig.bench_nkeys) {
    KeyType dummy_key = exist_keys[rand() % m];
    bench_keys.push_back(dummy_key);
    item_num++;
    //LOG(3) << "bench_key: " << dummy_key;
  }
  LOG(3) << "Load bench_keys size (uniform): " << bench_keys.size();
  return;
}

void read_file_data_with_no_duplicate(std::string path) {
  std::vector<uint64_t> vec;
  FILE *fin = fopen(path.c_str(), "rb");
  uint64_t item_num(0);
  int64_t buf[BUF_SIZE];
  fseek(fin,sizeof(uint64_t),SEEK_SET); //skip the two number:200M
  while (item_num < 3*BenConfig.nkeys/2) {
    size_t num_read = fread(buf, sizeof(uint64_t), BUF_SIZE, fin);
    for (size_t i = 0; i < num_read; i++) {
      vec.push_back(buf[i]);
      //LOG(3) << "exist_key: " << buf[i];
      item_num += num_read;
    }
    if (num_read < BUF_SIZE) break;
  }
  exist_keys = std::move(vec);
  // filter the duplicated one and shuffle it again
  std::sort(exist_keys.begin(), exist_keys.end());
  auto last = std::unique(exist_keys.begin(), exist_keys.end());
  exist_keys.erase(last, exist_keys.end());
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(exist_keys.begin(), exist_keys.end(), g);
  LOG(3) << "Load exist_keys size: " << exist_keys.size();

  item_num = 0;
  while (item_num < 3*BenConfig.non_nkeys/2) {
    size_t num_read = fread(buf, sizeof(uint64_t), BUF_SIZE, fin);
    for (size_t i = 0; i < num_read; i++) {
      nonexist_keys.push_back(buf[i]);
      // LOG(3) << "nonexist_key: " << buf[i];
      item_num += num_read;
    }
    if (num_read < BUF_SIZE) break;
  }
  fclose(fin);
  std::sort(nonexist_keys.begin(), nonexist_keys.end());
  last = std::unique(nonexist_keys.begin(), nonexist_keys.end());
  nonexist_keys.erase(last, nonexist_keys.end());
  std::shuffle(nonexist_keys.begin(), nonexist_keys.end(), g);
  LOG(3) << "Load non_exist_keys size: " << nonexist_keys.size();

  item_num = 0;
  size_t m = exist_keys.size();
  while (item_num < BenConfig.bench_nkeys) {
    KeyType dummy_key = exist_keys[rand() % m];
    bench_keys.push_back(dummy_key);
    item_num++;
    //LOG(3) << "bench_key: " << dummy_key;
  }
  LOG(3) << "Load bench_keys size (uniform): " << bench_keys.size();
  return;
}

void wiki_ts_data() {
  std::string path="../datasets/wiki_ts_200M_uint64";
  read_file_data_with_no_duplicate(path);
}

void fb_data() {
  std::string path="../datasets/fb_200M_uint64";
  read_file_data(path);
}

void osm_data() {
  std::string path="../datasets/osm_cellids_200M_uint64";
  read_file_data(path);
}

void book_data() {
  std::string path="../datasets/books_200M_uint64";
  read_file_data(path);
}

}

