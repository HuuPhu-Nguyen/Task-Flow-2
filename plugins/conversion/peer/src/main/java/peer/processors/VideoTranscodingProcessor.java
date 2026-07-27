package peer.processors;

import protocol.PayloadLimits;
import com.google.gson.Gson;
import conversion.model.FilePayload;
import objectstore.ObjectStoreProvider;
import objectstore.ObjectStores;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import peer.engine.TaskProcessor;
import protocol.SafeFileNames;
import protocol.TaskAssignMessage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;

import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_AAC;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_FLV1;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_MPEG4;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_MP3;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_OPUS;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_VP8;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_WMAV2;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_WMV2;
import static org.bytedeco.ffmpeg.global.avutil.AV_PIX_FMT_YUV420P;

public class VideoTranscodingProcessor implements TaskProcessor<FilePayload> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VideoTranscodingProcessor.class);

    private final Gson gson = new Gson();
    private final ObjectStoreProvider objectStoreProvider;

    public VideoTranscodingProcessor() {
        this(ObjectStores::open);
    }

    public VideoTranscodingProcessor(ObjectStoreProvider objectStoreProvider) {
        this.objectStoreProvider = Objects.requireNonNull(objectStoreProvider, "objectStoreProvider");
    }

    @Override
    public FilePayload process(TaskAssignMessage task) throws Exception {
        String targetFormat = task.getParam().toLowerCase();

        FilePayload input = gson.fromJson(gson.toJson(task.getPayload()), FilePayload.class);
        byte[] rawBytes = ObjectBackedPayloadReader.readInput(
                input,
                "Video",
                objectStoreProvider
        );
        if (rawBytes.length == 0) {
            throw new IOException("Video task decoded to an empty file: " + input.fileName());
        }

        String inputFileName = SafeFileNames.sanitize(input.fileName());
        String inputExt = getExtension(inputFileName);
        File tempIn = File.createTempFile("video_in_", inputExt);
        File tempOut = File.createTempFile("video_out_", "." + targetFormat);

        try {
            try (FileOutputStream fos = new FileOutputStream(tempIn)) {
                fos.write(rawBytes);
            }

            transcodeVideo(tempIn, tempOut, targetFormat);

            long outputSize = Files.size(tempOut.toPath());
            long maxResultBytes = PayloadLimits.maxResultBytes();
            if (outputSize > maxResultBytes) {
                throw new IOException("Video result exceeds " + PayloadLimits.MAX_RESULT_BYTES_ENV
                        + " (" + maxResultBytes + " bytes): " + input.fileName());
            }
            String newFileName = stripExtension(inputFileName) + "." + targetFormat;
            return ConversionOutputPublisher.publish(
                    task,
                    newFileName,
                    videoContentType(targetFormat),
                    tempOut.toPath(),
                    objectStoreProvider
            );
        } finally {
            deleteTempFile(tempIn);
            deleteTempFile(tempOut);
        }
    }

    private void deleteTempFile(File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            LOGGER.warn("event=temp_file_delete_failed file={} error={}",
                    file.getAbsolutePath(), e.getMessage(), e);
        }
    }

    private void transcodeVideo(File input, File output, String targetFormat) throws Exception {
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
            int audioChannels = grabber.getAudioChannels();
            int sampleRate = grabber.getSampleRate();
            boolean hasAudio = audioChannels > 0 && sampleRate > 0;

            try (FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(
                    output.getAbsolutePath(), width, height, hasAudio ? audioChannels : 0)) {
                recorder.setFormat(formatName);
                recorder.setVideoCodec(getVideoCodec(targetFormat));
                recorder.setPixelFormat(AV_PIX_FMT_YUV420P);
                recorder.setFrameRate(frameRate);
                recorder.setGopSize((int) Math.round(frameRate * 2));
                recorder.setVideoOption("preset", "medium");
                recorder.setVideoOption("crf", "23");
                if (hasAudio) {
                    recorder.setAudioChannels(audioChannels);
                    recorder.setSampleRate(sampleRate);
                    recorder.setAudioCodec(getAudioCodec(targetFormat));
                    int audioBitrate = grabber.getAudioBitrate();
                    recorder.setAudioBitrate(audioBitrate > 0 ? audioBitrate : 128_000);
                }
                recorder.start();

                Frame frame;
                while ((frame = grabber.grab()) != null) {
                    if (!hasAudio && frame.image == null && frame.samples != null) {
                        continue;
                    }
                    if (frame.image == null && frame.samples == null) {
                        continue;
                    }
                    recorder.setTimestamp(grabber.getTimestamp());
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

    private int getAudioCodec(String ext) {
        return switch (ext) {
            case "webm" -> AV_CODEC_ID_OPUS;
            case "wmv" -> AV_CODEC_ID_WMAV2;
            case "avi" -> AV_CODEC_ID_MP3;
            default -> AV_CODEC_ID_AAC;
        };
    }

    private String videoContentType(String ext) {
        return switch (ext) {
            case "avi" -> "video/x-msvideo";
            case "mkv" -> "video/x-matroska";
            case "mov" -> "video/quicktime";
            case "wmv" -> "video/x-ms-wmv";
            default -> "video/" + ext;
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
