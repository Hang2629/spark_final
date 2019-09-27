package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdBlacklistDAO;
import com.ibeifeng.sparkproject.domain.AdBlacklist;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import scala.collection.immutable.List;

import java.sql.ResultSet;
import java.util.ArrayList;

public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    @Override
    public void insertBatch(ArrayList<AdBlacklist> adBlacklists) {
        String sql = "INSERT INTO ad_blacklist VALUES(?)";

        ArrayList<Object[]> paramsList = new ArrayList<Object[]>();

        for(AdBlacklist adBlacklist : adBlacklists) {
            Object[] params = new Object[]{adBlacklist.getUserid()};
            paramsList.add(params);
        }

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }

    @Override
    public ArrayList<AdBlacklist> findAll() {
        String sql = "SELECT * FROM ad_blacklist";

        final ArrayList<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {

            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()) {
                    long userid = Long.valueOf(String.valueOf(rs.getInt(1)));

                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userid);

                    adBlacklists.add(adBlacklist);
                }
            }
        });

        return adBlacklists;
    }
}
