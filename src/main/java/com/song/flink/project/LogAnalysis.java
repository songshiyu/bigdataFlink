package com.song.flink.project;

import com.song.flink.project.constant.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import scala.Tuple4;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author songshiyu
 * @date 2020/2/3 15:18
 *
 *  读取kafka的数据
 *  读取mysql的数据
 *  connect
 *
 *  业务逻辑的处理分析：水印  WindowFunction
 *  ==》 ES 注意数据类型
 **/
@Slf4j
public class LogAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //因为要计算的是每个时间段内产生的流量，所以要给予event_time来处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //接收kafka数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaConstant.BOOTSTARTSTRAP_SERVERS);
        properties.setProperty("group.id", "test-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(KafkaConstant.TOPIC, new SimpleStringSchema(), properties);

        //接收kafka数据
        DataStreamSource<String> data = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> mapData =
                data.map(new MapFunction<String, Tuple4<String, Long, String, String>>() {
                    public Tuple4<String, Long, String, String> map(String value) throws Exception {
                        String[] splits = value.split("\t", -1);
                        Tuple4<String, Long, String, String> tuple4 = null;
                        Long time = 0L;
                        try {
                            String level = splits[2];
                            String timeStr = splits[3];
                            time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeStr).getTime();
                            String domain = splits[5];
                            String traffic = splits[6];
                            tuple4 = new Tuple4<String, Long, String, String>(level, time, domain, traffic);
                        } catch (Exception e) {
                            log.error("日志格式不标准:{}", value);
                        }
                        return tuple4;
                    }
                }).filter(new FilterFunction<Tuple4<String, Long, String, String>>() {
                    @Override
                    public boolean filter(Tuple4<String, Long, String, String> tuple4) throws Exception {
                        if (tuple4._2() != 0 && "M".equalsIgnoreCase(tuple4._1())) {
                            return true;
                        }
                        return false;
                    }
                }).map(new MapFunction<Tuple4<String, Long, String, String>, Tuple3<Long, String, Long>>() {
                    @Override
                    public Tuple3<Long, String, Long> map(Tuple4<String, Long, String, String> tuple4) throws Exception {
                        return new Tuple3<Long, String, Long>(tuple4._2(), tuple4._3(), Long.parseLong(tuple4._4()));
                    }
                });

        //使用flink的WarterMarks为每条记录添加水印(event_time)
        SingleOutputStreamOperator<Tuple3<String, String, Long>> apply = mapData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Long, String, Long>>() {

            Long maxOutOfOrderness = 10000L;
            Long currnetMaxTimestamp = 0L;

            @Override
            public long extractTimestamp(Tuple3<Long, String, Long> element, long previousElementTimestamp) {
                Long timestamp = element.f0;
                currnetMaxTimestamp = Math.max(timestamp, previousElementTimestamp);
                return timestamp;
            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currnetMaxTimestamp - maxOutOfOrderness);
            }
        }).keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .apply(new WindowFunction<Tuple3<Long, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, String, Long>> input, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        /**
                         * 输出：
                         *      第一个参数：这一分钟的时间
                         *      第二个参数：域名
                         *      第三个参数：traffic的和
                         */
                        String domain = tuple.getField(0).toString();
                        Long sum = 0L;
                        String currnetTime = "";

                        Iterator<Tuple3<Long, String, Long>> iterator = input.iterator();
                        while (iterator.hasNext()) {
                            Tuple3<Long, String, Long> next = iterator.next();
                            sum += next.f2;
                            //此处是能拿到这个window里边的时间的
                            if (org.apache.commons.lang3.StringUtils.isEmpty(currnetTime)) {
                                currnetTime = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(next.f0);
                            }
                        }

                        out.collect(new Tuple3<String, String, Long>(currnetTime, domain, sum));
                    }
                });

        /**
         * 添加ES Sink
         * */
        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("192.168.137.10", 9200, "http"));

        ElasticsearchSink.Builder<Tuple3<String, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple3<String, String, Long>>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple3<String, String, Long>>() {

                    @Override
                    public void process(Tuple3<String, String, Long> longStringLongTuple3, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(longStringLongTuple3));
                    }

                    public IndexRequest createIndexRequest(Tuple3<String, String, Long> element) {
                        Map<String, Object> json = new HashMap<String, Object>();
                        json.put("time", element.f0);
                        json.put("domain", element.f1);
                        json.put("traffic", element.f2);

                        String id = element.f0 + "-" + element.f1;

                        return Requests.indexRequest()
                                .index("cdh")
                                .type("traffic")
                                .id(id)
                                .source(json);
                    }
                }
        );

        apply.addSink(esSinkBuilder.build());

        apply.print().setParallelism(1);

        env.execute("LogAnalysis");
    }
}
