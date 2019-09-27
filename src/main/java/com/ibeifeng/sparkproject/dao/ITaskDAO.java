package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.Task;

public interface ITaskDAO {

    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    Task findById(long taskid);
}
