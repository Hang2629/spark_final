package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdBlacklist;
import scala.collection.immutable.List;

import java.util.ArrayList;

public interface IAdBlacklistDAO {
    /**
     * 批量插入广告黑名单用户
     * @param adBlacklists
     */
    void insertBatch(ArrayList<AdBlacklist> adBlacklists);

    /**
     * 查询所有广告黑名单用户
     * @return
     */
    ArrayList<AdBlacklist> findAll();
}
