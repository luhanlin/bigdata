package com.luhanlin.spark.warn.dao;

import com.bigdata.common.db.DBCommon;
import com.luhanlin.spark.warn.domain.TZ_RuleDomain;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-04-25 00:57
 */
public class TZ_RuleDao {

    private static final Logger LOG = LoggerFactory.getLogger(TZ_RuleDao.class);

    /**
     *  获取所有的规则
     * @return
     */
    public static List<TZ_RuleDomain> getRuleList(){
        List<TZ_RuleDomain> listRules = null;
        //获取连接
        Connection conn = DBCommon.getConn("test");
        //执行器
        QueryRunner query = new QueryRunner();
        String sql = "select * from tz_rule";
        try {
            listRules = query.query(conn,sql,new BeanListHandler<>(TZ_RuleDomain.class));
        } catch (SQLException e) {
            LOG.error(null,e);
        }finally {
            DBCommon.close(conn);
        }
        return listRules;
    }


    public static void main(String[] args) {
        List<TZ_RuleDomain> ruleList = TZ_RuleDao.getRuleList();
        System.out.println(ruleList.size());
        ruleList.forEach(x->{
            System.out.println(x);
        });
    }

}
