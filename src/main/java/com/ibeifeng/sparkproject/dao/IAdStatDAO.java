package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdStat;

import java.util.List;

public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStats);

}
