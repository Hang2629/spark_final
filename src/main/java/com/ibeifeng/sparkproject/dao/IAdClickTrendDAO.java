package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdClickTrend;
import java.util.List;

public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
