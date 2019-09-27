package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.SessionDetail;
import java.util.List;

public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);

    /**
     * 批量插入session明细数据
     * @param sessionDetails
     */
    void insertBatch(List<SessionDetail> sessionDetails);

}
