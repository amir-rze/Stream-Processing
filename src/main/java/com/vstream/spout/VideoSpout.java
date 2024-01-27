package com.vstream.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.opencv.core.*;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;
import org.opencv.videoio.Videoio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class VideoSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private VideoCapture capture;
    public static int frameCount ;
    private double brightnessSum = 0;
    private int frameCounter = 0;
    private ArrayList <Double> frameBrightnessList = new ArrayList<>();

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("frame","frameCounter"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        capture = new VideoCapture("video.mp4");
        frameCount =  (int) capture.get(7) - 6;
    }

    @Override
    public void nextTuple() {
        Mat frame = new Mat();
        capture.read(frame);

        if (frameCounter < frameCount) {
            // Increment frame count
            frameCounter++;
            // Calculate brightness
            Mat grayFrame = new Mat();
            Imgproc.cvtColor(frame, grayFrame, Imgproc.COLOR_BGR2GRAY);
            double brightness = Core.mean(grayFrame).val[0];
            frameBrightnessList.add(brightness);
            brightnessSum += brightness;

            // Resize frame
            Size size = new Size(640, 480);
            Mat resizedFrame = new Mat();
            Imgproc.resize(grayFrame, resizedFrame, size);

            // Write frame to file
            String filename = "frames/frame" + frameCounter + ".jpg";
            Imgcodecs.imwrite(filename, resizedFrame);

            collector.emit(new Values(resizedFrame , frameCounter));
        }
    }

    @Override
    public void ack(Object id) {
        System.out.println("Successfully done!");
    }

    @Override
    public void fail(Object id) {
        System.out.println("Failed!");
    }

    @Override
    public void close() {
        // Calculate average brightness
        double averageBrightness = brightnessSum / frameCount;
        System.out.println("Total number of frames: " + frameCount);
        System.out.println("Total Average brightness: " + averageBrightness);

        // Write metrics to file
        String filename = "metrics.txt";
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
            writer.write("Total number of frames: " + frameCount + "\n");
            writer.write("Total Average brightness: " + averageBrightness + "\n" + "-------***********-------" + "\n");
            for (int i = 0; i <frameCount ; i++) {
                writer.write("Average Brightness of frame #" + (i+1) + " = " + frameBrightnessList.get(i) + "\n");
            }
            writer.close();
        } catch (IOException e) {
            System.out.println("An error occurred while writing metrics to file.");
            e.printStackTrace();
        }
    }
}
