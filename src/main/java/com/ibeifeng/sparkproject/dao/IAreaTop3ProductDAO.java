package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AreaTop3Product;
import scala.collection.immutable.List;

import java.util.ArrayList;

public interface IAreaTop3ProductDAO {
    void insertBatch(ArrayList<AreaTop3Product> areaTopsProducts);
}
