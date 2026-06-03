package peer.processors;

import com.google.gson.Gson;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import peer.engine.TaskProcessor;
import protocol.FilePayload;
import protocol.TaskAssignMessage;

import java.io.*;
import java.nio.file.Files;
import java.util.Base64;

import static org.bytedeco.ffmpeg.global.avcodec.*;
import static org.bytedeco.ffmpeg.global.avutil.AV_PIX_FMT_YUV420P;

/**
 * Video transcoding processor using JavaCV (bundles FFmpeg).
 * Works on all platforms without requiring FFmpeg installation.
 */
public class VideoTranscodingProcessor implements TaskProcessor<FilePayload> {
    private final Gson gson = new Gson();

    @Override
    public FilePayload process(TaskAssignMessage task) throws Exception {
        String targetFormat = task.getParam().toLowerCase();

        FilePayload input = gson.fromJson(gson.toJson(task.getPayload()), FilePayload.class);
        if (input == null || input.base64Data() == null || input.base64Data().isBlank()) {
            throw new IOException("Video task has no input data.");
        }
        
        // Decode input from Base64
        byte[] rawBytes = Base64.getDecoder().decode(input.base64Data());
        if (rawBytes.length == 0) {
            throw new IOException("Video task decoded to an empty file: " + input.fileName());
        }
        
        // Create temp input file
        String inputExt = getExtension(input.fileName());
        File tempIn = File.createTempFile("video_in_", inputExt);
        File tempOut = File.createTempFile("video_out_", "." + targetFormat);
        
        try {
            // Write input bytes to temp file
            try (FileOutputStream fos = new FileOutputStream(tempIn)) {
                fos.write(rawBytes);
            }

            // Transcode using JavaCV
            transcodeVideo(tempIn, tempOut, targetFormat);

            // Read output and encode to Base64
            byte[] outputBytes = Files.readAllBytes(tempOut.toPath());
            String outBase64 = Base64.getEncoder().encodeToString(outputBytes);

            String newFileName = stripExtension(input.fileName()) + "." + targetFormat;
            return new FilePayload(newFileName, outBase64);
            
        } finally {
            // Cleanup temp files
            if (tempIn.exists()) tempIn.delete();
            if (tempOut.exists()) tempOut.delete();
        }
    }

    private void transcodeVideo(File input, File output, String targetFormat) throws Exception {
        // Map format names to FFmpeg codec names
        String formatName = getFormatName(targetFormat);
        
        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(input)) {
            grabber.start();
            
            int width = grabber.getImageWidth();
            int height = grabber.getImageHeight();
            if (width <= 0 || height <= 0) {
                throw new IOException("Could not read video dimensions from " + input.getName());
            }

            double frameRate = grabber.getFrameRate();
            if (Double.isNaN(frameRate) || frameRate <= 0) {
                frameRate = 30.0;
            }
            
            try (FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(
                    output.getAbsolutePath(), width, height)) {
                
                recorder.setFormat(formatName);
                recorder.setVideoCodec(getVideoCodec(targetFormat));
                recorder.setPixelFormat(AV_PIX_FMT_YUV420P);
                recorder.setFrameRate(frameRate);
                recorder.setGopSize((int) Math.round(frameRate * 2));
                recorder.setVideoOption("preset", "medium");
                recorder.setVideoOption("crf", "23");
                recorder.start();
                
                Frame frame;
                while ((frame = grabber.grabImage()) != null) {
                    recorder.record(frame);
                }
                
                recorder.stop();
            }
            
            grabber.stop();
        }
    }

    private String getFormatName(String ext) {
        return switch (ext) {
            case "mp4" -> "mp4";
            case "avi" -> "avi";
            case "mkv" -> "matroska";
            case "mov" -> "mov";
            case "webm" -> "webm";
            case "flv" -> "flv";
            case "wmv" -> "wmv";
            default -> "mp4";
        };
    }

    private int getVideoCodec(String ext) {
        return switch (ext) {
            case "webm" -> AV_CODEC_ID_VP8;
            case "flv" -> AV_CODEC_ID_FLV1;
            case "wmv" -> AV_CODEC_ID_WMV2;
            default -> AV_CODEC_ID_MPEG4;
        };
    }

    private String getExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return (dot == -1) ? ".mp4" : fileName.substring(dot);
    }

    private String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return (dot == -1) ? fileName : fileName.substring(0, dot);
    }
}
