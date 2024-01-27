package com.vstream.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.vstream.spout.VideoSpout;
import org.opencv.core.Mat;
import org.opencv.core.Core;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.videoio.VideoWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class AggregationOutputCreationBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Map<Integer, Mat> blurredFrames;
    private Map<Integer, Mat> sharpenedFrames;
    private List<Mat> aggregatedFrames;
    private BufferedWriter writer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        this.blurredFrames = new TreeMap<>();
        this.sharpenedFrames = new TreeMap<>();
        this.aggregatedFrames = new ArrayList<>();
        try {
            writer = new BufferedWriter(new FileWriter("finalMetrics.txt"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String source = tuple.getStringByField("source");
        Mat frame = (Mat) tuple.getValueByField("frame");
        int frameCounter = tuple.getIntegerByField("frameCounter");

        if (source.equals("gaussianBlurBolt")) {
            blurredFrames.put(frameCounter, frame);
        } else {
            sharpenedFrames.put(frameCounter, frame);
        }

        if (blurredFrames.size() == VideoSpout.frameCount && sharpenedFrames.size() == VideoSpout.frameCount) {
            List<Mat> blurredFramesList = new ArrayList<>(blurredFrames.values());
            List<Mat> sharpenedFramesList = new ArrayList<>(sharpenedFrames.values());
            Mat blurredFrame;
            Mat sharpenedFrame;
            double totalBrightness = 0;
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter("finalMetrics.txt"));
                for (int i = 0; i < VideoSpout.frameCount; i++) {
                    blurredFrame = blurredFramesList.get(i);
                    sharpenedFrame = sharpenedFramesList.get(i);
                    Mat aggregatedFrame = new Mat();
                    Core.add(blurredFrame, sharpenedFrame, aggregatedFrame);

                    String fileName = "AggregatedFrames/frame" + (i + 1) + ".jpg";
                    Imgcodecs.imwrite(fileName, aggregatedFrame);

                    aggregatedFrames.add(aggregatedFrame);
                    double brightness = Core.mean(aggregatedFrame).val[0];
                    totalBrightness += brightness;
                    writer.write("Frame #" + (i + 1) + " brightness: " + brightness + "\n");
                }
                writer.write("\n\n--------***************---------\n\n");
                writer.write("Total number of frames: " + VideoSpout.frameCount + "\n");
                writer.write("Total average brightness: " + totalBrightness / VideoSpout.frameCount + "\n");
                writer.close();

                createVideo(aggregatedFrames);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        collector.ack(tuple);
    }

    private void createVideo(List<Mat> frames) {
        File outputFile = new File("outputVideo.mp4");
        int fourcc = VideoWriter.fourcc('m', 'p', '4', 'v');
        int fps = 30;
        Size frameSize = new Size(frames.get(0).cols(), frames.get(0).rows());
        VideoWriter videoWriter = new VideoWriter(outputFile.getAbsolutePath(), fourcc, fps, frameSize);
        for (Mat frame : frames) {
            videoWriter.write(frame);
        }
        videoWriter.release();

    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}


