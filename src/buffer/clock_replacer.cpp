//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    frame_sets = new frame_id_t[num_pages];
    ref_bits = new size_t[num_pages];
    for(int i=0;i<num_pages;i++) {
      frame_sets[i] = 0;
      ref_bits[i] = 0;
    }
    clock_hand = 0;
    num_page = num_pages;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
  bool flag=false;
  frame_id=NULL;
  for(int i=0;i<num_page;i++) {
    size_t index = clock_hand%num_page;
    if(frame_sets[index]>0) {
      if(ref_bits[index]==1) {
        ref_bits[index]=0;
      }else {
        *frame_id = index;
        flag=true;
        break;
      }
    }else {
     clock_hand++;
    }
  }

  if(!flag) {
    for(int i=0;i<num_page;i++) {
      size_t index = clock_hand%num_page;
      if(frame_sets[index]>0) {
        *frame_id = index;
        flag=true;
        break;
      }else {
        clock_hand++;
      }
    }
  }
  return flag;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  ref_bits[frame_id] = ref_bits[frame_id]+1;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  ref_bits[frame_id] = ref_bits[frame_id]-1;
  //  frame_sets[frame_id] =frame_sets[frame_id]-1;
}

size_t ClockReplacer::Size() { 
  size_t num=0;
  for(int i=0;i<num_page;i++) {
    if(frame_sets[i]>0) num++;
  }
  return num; 
  }

}  // namespace bustub
