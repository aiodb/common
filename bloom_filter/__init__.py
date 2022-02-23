#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2022/2/23
import math

import mmh3
from bitarray import bitarray


class BloomFilter:

    def __init__(self, item_amount: int, fp_prob: float):
        """
        :param item_amount: amount of all elements to filter
        :param fp_prob: false positive probability
        """
        self.size = int(-(item_amount * math.log(fp_prob)) / (math.log(2) ** 2))
        self.fp_prob = fp_prob
        self.hash_count = int((self.size / item_amount) * math.log(2))
        self.bitarray = bitarray(self.size)
        self.bitarray.setall(0)

    def add(self, item):
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            self.bitarray[digest] = True

    def check(self, item) -> bool:
        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            if not self.bitarray[digest]:
                return False
        return True
