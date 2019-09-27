package com.ibeifeng.sparkproject.ad;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IAdBlacklistDAO;
import com.ibeifeng.sparkproject.dao.IAdClickTrendDAO;
import com.ibeifeng.sparkproject.dao.IAdProvinceTop3DAO;
import com.ibeifeng.sparkproject.dao.IAdStatDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.AdBlacklist;
import com.ibeifeng.sparkproject.domain.AdClickTrend;
import com.ibeifeng.sparkproject.domain.AdProvinceTop3;
import com.ibeifeng.sparkproject.domain.AdStat;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class AdClickRealTimeStatSpark {
    public static void main (String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AdClickRealTimeStatSpark")
                .setMaster("local[2]")
                .set("spark.sql.warehouse.dir","C:\\Users\\Administrator\\Desktop\\spark-warehouse");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<String>();
        for(String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams, topics);


        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(adRealTimeLogDStream);


        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
        // 最粗
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);


        // 业务功能二：实时统计每天每个省份top3热门广告
        // 统计的稍微细一些了
        calculateProvinceTop3Ad(adRealTimeStatDStream);


        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        // 统计的非常细了
        // 我们每次都可以看到每个广告，最近一小时内，每分钟的点击量
        // 每支广告的点击趋势
        calculateAdClickCountByWindow(adRealTimeLogDStream);

    }

    private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 映射成<yyyyMMddHHMM_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(

                new PairFunction<Tuple2<String,String>, String, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        // timestamp province city userid adid
                        String[] logSplited = tuple._2.split(" ");
                        String timeMinute = DateUtils.formatTimeMinute(
                                new Date(Long.valueOf(logSplited[0])));
                        long adid = Long.valueOf(logSplited[4]);

                        return new Tuple2<String, Long>(timeMinute + "_" + adid, 1L);
                    }

                });

        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        }, Durations.minutes(60), Durations.seconds(10));

        aggrRDD.foreachRDD(new VoidFunction<JavaPairRDD<String,Long>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator)
                            throws Exception {
                        List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();

                        while(iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            // yyyyMMddHHmm
                            String dateMinute = keySplited[0];
                            long adid = Long.valueOf(keySplited[1]);
                            long clickCount = tuple._2;

                            String date = DateUtils.formatDate(DateUtils.parseDateKey(
                                    dateMinute.substring(0, 8)));
                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAdid(adid);
                            adClickTrend.setClickCount(clickCount);

                            adClickTrends.add(adClickTrend);
                        }

                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                        adClickTrendDAO.updateBatch(adClickTrends);
                    }

                });
            }
        });
    }


    private static void calculateProvinceTop3Ad(JavaPairDStream<String,Long> adRealTimeStatDStream) {
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                        String[] keySplited = tuple._1.split("_");
                        String date = keySplited[0];
                        String province = keySplited[1];
                        long adid = Long.valueOf(keySplited[3]);
                        long clickCount = tuple._2;

                        String key = date + "_" + province + "_" + adid;

                        return new Tuple2<String, Long>(key, clickCount);
                    }
                });

                JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long aLong, Long aLong2) throws Exception {
                        return aLong + aLong2;
                    }
                });


                JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(

                        new Function<Tuple2<String, Long>, Row>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public Row call(Tuple2<String, Long> tuple)
                                    throws Exception {
                                String[] keySplited = tuple._1.split("_");
                                String datekey = keySplited[0];
                                String province = keySplited[1];
                                long adid = Long.valueOf(keySplited[2]);
                                long clickCount = tuple._2;

                                String date = DateUtils.formatDate(DateUtils.parseDateKey(datekey));

                                return RowFactory.create(date, province, adid, clickCount);
                            }

                        });

                StructType schema = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("date", DataTypes.StringType, true),
                        DataTypes.createStructField("province", DataTypes.StringType, true),
                        DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                        DataTypes.createStructField("click_count", DataTypes.LongType, true)));

                HiveContext sqlContext = new HiveContext(rdd.context());
                Dataset<Row> dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);
                dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");

                String sql = "SELECT "
                        + "date,"
                        + "province,"
                        + "ad_id,"
                        + "click_count "
                        + "FROM ( "
                        + "SELECT "
                        + "date,"
                        + "province,"
                        + "ad_id,"
                        + "click_count,"
                        + "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
                        + "FROM tmp_daily_ad_click_count_by_prov "
                        + ") t "
                        + "WHERE rank>=3";

                Dataset<Row> sql1 = sqlContext.sql(sql);
                return sql1.javaRDD();
            }
        });

        // 将其中的数据批量更新到MySQL中
        rowsDStream.foreachRDD(new VoidFunction<JavaRDD<Row>> () {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<Row> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();

                        while(iterator.hasNext()) {
                            Row row = iterator.next();
                            String date = row.getString(0);
                            String province = row.getString(1);
                            long adid = row.getLong(2);
                            long clickCount = row.getLong(3);

                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            adProvinceTop3.setDate(date);
                            adProvinceTop3.setProvince(province);
                            adProvinceTop3.setAdid(adid);
                            adProvinceTop3.setClickCount(clickCount);

                            adProvinceTop3s.add(adProvinceTop3);
                        }

                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                    }

                });
            }
        });
    }

    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
                private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                String log = tuple._2;
                String[] logSplited = log.split(" ");
                String timestamp = logSplited[0];
                Date date = new Date(Long.valueOf(timestamp));
                String datekey = DateUtils.formatDateKey(date);	// yyyyMMdd

                String province = logSplited[1];
                String city = logSplited[2];
                long adid = Long.valueOf(logSplited[4]);

                String key = datekey + "_" + province + "_" + city + "_" + adid;
                return new Tuple2<String, Long>(key, 1L);
            }
        });
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
                long clickcount = 0L;
                if(optional.isPresent()) {
                    clickcount = optional.get();
                }
                for (Long val : values) {
                    clickcount += val;
                }
                return Optional.of(clickcount);
            }
        });

        // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
        aggregatedDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Long>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator)
                            throws Exception {
                        List<AdStat> adStats = new ArrayList<AdStat>();

                        while(iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split("_");
                            String date = keySplited[0];
                            String province = keySplited[1];
                            String city = keySplited[2];
                            long adid = Long.valueOf(keySplited[3]);

                            long clickCount = tuple._2;

                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAdid(adid);
                            adStat.setClickCount(clickCount);

                            adStats.add(adStat);
                        }

                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                        adStatDAO.updateBatch(adStats);
                    }

                });
            }
        });
    return aggregatedDStream;
    }

    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {

        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =  adRealTimeLogDStream.transformToPair(new Function<JavaPairRDD<String,String>,JavaPairRDD<String,String>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
                IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                ArrayList<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

                ArrayList<Tuple2<Long, Boolean>> tuples = new ArrayList<>();
                for (AdBlacklist al : adBlacklists) {
                    tuples.add(new Tuple2<Long, Boolean>(al.getUserid(), true));
                }
                JavaSparkContext sc = new JavaSparkContext(rdd.context());
                JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                // 将原始数据rdd映射成<userid, tuple2<string, string>>
                JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Tuple2<String, String>> call(
                            Tuple2<String, String> tuple)
                            throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");
                        long userid = Long.valueOf(logSplited[3]);
                        return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
                    }

                });

                JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);
                JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                        Optional<Boolean> optional = tuple._2._2;
                        if (optional.isPresent() && optional.get()) {
                            return false;
                        }
                        return true;
                    }
                });

                JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(

                        new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public Tuple2<String, String> call(
                                    Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
                                    throws Exception {
                                return tuple._2._1;
                            }

                        });

                return resultRDD;
            }

        });
        return filteredAdRealTimeLogDStream;
    }

}
