package ideaflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class demo {

    public static void main(String[] args) throws Exception {
        //定义执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //定义数据流来源
        DataStream<Tuple2<String, Integer>> dataStream = env
                //.readTextFile("/mnt/hgfs/Share/flink/q.txt")
                .readTextFile("B:/Share/flink/q.txt")
                .flatMap(new Splitter()).keyBy(0).sum(1);


        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String,Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}