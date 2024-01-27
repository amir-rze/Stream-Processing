# Stream Processing With Apache Storm

## Objective
Develop a stream processing application using Apache Storm to perform real-time image processing on video files. The application will demonstrate distributed computing principles by
processing video frames in parallel, applying image filters, and aggregating the results.
Framework and Tools

• Primary Framework: Apache Storm
• Programming Language: Java (recommended) or any other suitable language
• Additional Libraries: Image processing libraries (e.g., OpenCV for Java)

## Functional Requirements
1. Video File Reading: Implement a mechanism to read video files. Extract individual
frames from the video.
2. Frame Analysis: Count the total number of frames in the video. Calculate and record
the average brightness of each frame.
3. Image Processing Operations: Convert frames to grayscale. Resize frames to a predetermined smaller size.
4. Distributed Image Filtering: Use Apache Storm’s distributed processing capabilities.
Create two types of processing elements (PEs): PE1 applies a Gaussian blur filter and
PE2 applies a sharpening filter to the frame.
5. Frame Aggregation: Develop a PE that combines the output of PE1 and PE2. Sum
up the matrices resulting from the applied filters.
6. Output Creation: Generate a new video file with the filtered frames. Produce a text
file containing frame analysis data (average brightness, frame count).


![Bolts](https://github.com/amir-rze/Stream-Processing/assets/37247427/674d7aab-2f2a-4aee-8246-72467e378016)
