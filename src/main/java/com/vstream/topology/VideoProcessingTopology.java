package com.vstream.topology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import com.vstream.bolts.AggregationOutputCreationBolt;
import com.vstream.bolts.GaussianBlurBolt;
import com.vstream.bolts.SharpeningBolt;
import com.vstream.spout.VideoSpout;
import org.apache.storm.utils.Utils;

public class VideoProcessingTopology {
    static {
        // Load the OpenCV native library
        System.load("C:/Tools/opencv_java480.dll"); // For Windows
    }


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("videoSpout", new VideoSpout(), 1);

        builder.setBolt("gaussianBlurBolt", new GaussianBlurBolt(), 2)
                .shuffleGrouping("videoSpout");

        builder.setBolt("sharpeningBolt", new SharpeningBolt(), 2)
                .shuffleGrouping("videoSpout");

        builder.setBolt("aggregationOutputCreationBolt", new AggregationOutputCreationBolt(), 1)
                .shuffleGrouping("gaussianBlurBolt")
                .shuffleGrouping("sharpeningBolt");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("VideoProcessingTopology", config, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("VideoProcessingTopology");
        cluster.shutdown();
        return;
    }
}
