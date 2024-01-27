package com.vstream.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;
import org.opencv.imgcodecs.Imgcodecs;

import java.util.Map;

public class SharpeningBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        Mat frame = (Mat) tuple.getValueByField("frame");
        int frameCount = tuple.getIntegerByField("frameCounter");

        Mat sharpened = new Mat();
        Imgproc.GaussianBlur(frame, sharpened, new Size(0, 0), 3);
        Core.addWeighted(frame, 2.0, sharpened, -1.0, 0, sharpened);
        String filename = "sharpenedFrames/frame" + frameCount + ".jpg";
        Imgcodecs.imwrite(filename, sharpened);

        collector.emit(tuple, new Values("sharpeningBolt",sharpened , frameCount));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source","frame","frameCounter"));
    }
}