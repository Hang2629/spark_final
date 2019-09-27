package com.ibeifeng.sparkproject.product;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IAreaTop3ProductDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.AreaTop3Product;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AreaTop3ProductSpark {
    public static void main (String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AreaTop3ProductSpark")
                .set("spark.sql.warehouse.dir","C:\\Users\\Administrator\\Desktop\\spark-warehouse");
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //zhuce UDF

        sqlContext.udf().register("concat_long_string",new ConcatLongStringUDF(),DataTypes.StringType);
        sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(),DataTypes.StringType);
        sqlContext.udf().register("random_prefix", new RandomPrefixUDF(),DataTypes.StringType);
        sqlContext.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(),DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());


        // 准备模拟数据
        SparkUtils.mockData(sc, sqlContext);
        // 获取命令行传入的taskid，查询对应的任务参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        long taskid = ParamUtils.getTaskIdFromArgs(args,
                Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task = taskDAO.findById(taskid);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        // 技术点1：Hive数据源的使用
        JavaPairRDD<Long, Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(
                sqlContext, startDate, endDate);
        System.out.println("cityid2clickActionRDD: " + cityid2clickActionRDD.count());

        // 从MySQL中查询城市信息
        // 技术点2：异构数据源之MySQL的使用
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);

        // 生成点击商品基础信息临时表
        // 技术点3：将RDD转换为DataFrame，并注册临时表
        generateTempClickProductBasicTable(sqlContext,
                cityid2clickActionRDD, cityid2cityInfoRDD);


        // 生成各区域各商品点击次数的临时表
        generateTempAreaPrdocutClickCountTable(sqlContext);


        // 生成包含完整商品信息的各区域各商品点击次数的临时表
        generateTempAreaFullProductClickCountTable(sqlContext);


// 使用开窗函数获取各个区域内点击次数排名前3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);

        List<Row> rows = areaTop3ProductRDD.collect();
        persistAreaTop3Product(taskid, rows);
        sc.close();

    }

    private static void persistAreaTop3Product(long taskid, List<Row> rows) {
        ArrayList<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();

        for(Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskid);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));
            areaTop3Products.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areTop3ProductDAO.insertBatch(areaTop3Products);
    }

    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
        String sql = "select " +
                    " area, +" +
                " case " +
                "  when area='China North' or area='China East' then 'A level' " +
                " when area='China South' or area='China Middle' then 'B Level' " +
                " when area='West North' or area='West South' then 'C Level' " +
                " ELSE 'D Level' " +
                " end area_level, " +
                " product_id, " +
                " click_count, " +
                " city_infos, " +
                " product_name, " +
                " product_status " +
                " from ( " +
                       "select " +
                        "  area,product_id,click_count,city_infos,product_name,product_status, " +
                        "  row_number() over (partition by area  order by click_count desc ) rank" +
                        "  from tmp_area_fullprod_click_count" +
                        "   ) t" +
                "  where rank rank <= 3 ";

        Dataset<Row> sql1 = sqlContext.sql(sql);
        return sql1.javaRDD();
    }

    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        String sql = "select tapcc.area,tapcc.product_id,tapcc.click_count,tapcc.city_infos,pi.product_name," +
                " if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
                " from tmp_area_product_click_count tapcc " +
                " join product_info pi on tapcc.product_id = pi.product_id";
        Dataset<Row> sql1 = sqlContext.sql(sql);
        sql1.registerTempTable("tmp_area_fullprod_click_count");

    }

    private static void generateTempAreaPrdocutClickCountTable(SQLContext sqlContext) {
        String sql  = "select area,product_id,count(*), click_count," +
                " group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos " +
                " from tmp_click_product_basic " +
                " group by area,product_id";

        Dataset<Row> sql1 = sqlContext.sql(sql);
        sql1.registerTempTable("tmp_area_product_click_count");
    }

    private static void generateTempClickProductBasicTable(
            SQLContext sqlContext,
            JavaPairRDD<Long, Row> cityid2clickActionRDD,
            JavaPairRDD<Long, Row> cityid2cityInfoRDD) {
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityid2clickActionRDD.join(cityid2cityInfoRDD);

        JavaRDD<Row> mappedRDD = joinedRDD.map(

                new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple)
                            throws Exception {
                        long cityid = tuple._1;
                        Row clickAction = tuple._2._1;
                        Row cityInfo = tuple._2._2;

                        long productid = clickAction.getLong(1);
                        String cityName = cityInfo.getString(1);
                        String area = cityInfo.getString(2);

                        return RowFactory.create(cityid, cityName, area, productid);
                    }

                });

        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id",DataTypes.LongType,true));
        structFields.add(DataTypes.createStructField("city_name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("area",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("product_id",DataTypes.LongType,true));

        StructType schema = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(mappedRDD, schema);
        dataFrame.registerTempTable("tmp_click_product_basic");

    }

        private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
        // 构建MySQL连接配置信息（直接从配置文件中获取）
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if(local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", password);

        Dataset<Row> cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2cityInfoRDD =  cityInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                Long cityid = Long.valueOf(String.valueOf(row.get(0)));

                return new Tuple2<Long,Row>(cityid,row);
            }
        });

        return cityid2cityInfoRDD;
    }

        private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(
            SQLContext sqlContext, String startDate, String endDate) {
        String sql = "select city_id,click_product_id product_id" +
                " from user_visit_action  where click_product_id is not null" +
                " and date >= '" + startDate + "'" +
                " and date <='" + endDate + "'";

        Dataset<Row> clickActionDF = sqlContext.sql(sql);
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2clickActionRDD =  clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                Long cityId = row.getLong(0);
                return new Tuple2<Long,Row>(cityId,row);
            }
        });
        return cityid2clickActionRDD;
    }
}
