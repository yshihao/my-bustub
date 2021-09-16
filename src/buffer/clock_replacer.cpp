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
   ClockItem clockItem = {true,false};
  for(size_t i=0;i<num_pages;i++) {
    clock_replacr.emplace_back(clockItem);
  }
  clock_hand = 0;
  in_clock_size = 0; 
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
  std::scoped_lock locks{mylock};
  while(in_clock_size>0) {
    clock_hand = clock_hand % clock_replacr.size();
    if(clock_replacr[clock_hand].isPin){
      clock_hand++;
    }else if(clock_replacr[clock_hand].ref) {
      clock_replacr[clock_hand].ref = false;
      clock_hand++;
    }else {
      clock_replacr[clock_hand].isPin = true;
      // clock_replacr[clock_hand].ref = true;
      *frame_id = clock_hand;
      clock_hand++;
      in_clock_size--;
      return true;
    }
   }
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock locks{mylock};
  if(!clock_replacr[frame_id].isPin) {
    clock_replacr[frame_id].isPin = true;
    // clock_replacr[frame_id].ref = true;
    in_clock_size--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock locks{mylock};
  if(clock_replacr[frame_id].isPin) {
    clock_replacr[frame_id].isPin = false;
    clock_replacr[frame_id].ref = true;
    in_clock_size++;
  }
}

size_t ClockReplacer::Size() { 
  std::scoped_lock locks{mylock};

  return in_clock_size;

  }
}  // namespace bustub
