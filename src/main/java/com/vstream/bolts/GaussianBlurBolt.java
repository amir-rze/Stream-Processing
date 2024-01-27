package com.vstream.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.opencv.core.*;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoWriter;

import java.util.Map;

public class GaussianBlurBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Mat frame = (Mat) tuple.getValueByField("frame");
        int frameCounter = tuple.getIntegerByField("frameCounter");
        Mat blurred = new Mat();
        Imgproc.GaussianBlur(frame, blurred, new Size(0, 0), 3);


        String filename = "blurredFrames/frame" + frameCounter + ".jpg";
        Imgcodecs.imwrite(filename, blurred);

        collector.emit(tuple, new Values("gaussianBlurBolt", blurred, frameCounter));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "frame", "frameCounter"));
    }

}
