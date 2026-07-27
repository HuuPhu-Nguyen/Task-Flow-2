package peer.processors;

import conversion.model.ConversionTaskTypes;
import conversion.model.FilePayload;
import objectstore.ObjectReference;
import objectstore.TaskFlowObjectKeys;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import protocol.PayloadLimits;
import protocol.TaskAssignMessage;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.nio.ShortBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;

import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_AAC;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_MPEG4;
import static org.bytedeco.ffmpeg.global.avutil.AV_PIX_FMT_YUV420P;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VideoTranscodingProcessorTest {
    private static final int WIDTH = 32;
    private static final int HEIGHT = 32;
    private static final int SAMPLE_RATE = 8_000;
    private static final int SAMPLES_PER_FRAME = 800;

    @TempDir
    Path tempDir;

    @Test
    void preservesAudioStreamWhenTranscodingVideo() throws Exception {
        Path source = tempDir.resolve("source.mp4");
        createVideoWithAudio(source);

        FilePayload input = new FilePayload(
                "source.mp4",
                Base64.getEncoder().encodeToString(Files.readAllBytes(source)));
        TaskAssignMessage task = new TaskAssignMessage(
                "COORDINATOR",
                Instant.now().toString(),
                "task-1",
                "job-1",
                ConversionTaskTypes.VIDEO_TRANSCODING,
                1,
                "550e8400-e29b-41d4-a716-446655440000",
                1_780_000_000_000L,
                input,
                "mp4");

        FilePayload result = new VideoTranscodingProcessor().process(task);

        assertEquals("source.mp4", result.fileName());
        Path output = tempDir.resolve("transcoded.mp4");
        Files.write(output, Base64.getDecoder().decode(result.base64Data()));
        assertTrue(hasAudioSamples(output), "transcoded video should retain audio samples");
    }

    @Test
    void stagesTranscodedVideoUnderTheExactAssignmentKey() throws Exception {
        Path source = tempDir.resolve("staged-source.mp4");
        createVideoWithAudio(source);
        byte[] inputBytes = Files.readAllBytes(source);
        ProcessorMemoryObjectStore store = new ProcessorMemoryObjectStore();
        ObjectReference inputReference = new ObjectReference(
                TaskFlowObjectKeys.objectKey("inputs", "video-test"),
                inputBytes.length,
                HexFormat.of().formatHex(
                        MessageDigest.getInstance("SHA-256").digest(inputBytes)
                ),
                "video/mp4"
        );
        store.put(inputReference, new java.io.ByteArrayInputStream(inputBytes));
        FilePayload input = new FilePayload("source.mp4", null, inputReference);
        String assignmentId = "550e8400-e29b-41d4-a716-446655440000";
        TaskAssignMessage task = new TaskAssignMessage(
                "COORDINATOR",
                Instant.now().toString(),
                "task-video",
                "job-video",
                ConversionTaskTypes.VIDEO_TRANSCODING,
                1,
                assignmentId,
                1_780_000_000_000L,
                input,
                "mp4"
        );
        System.setProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY, "1");
        try {
            FilePayload result = new VideoTranscodingProcessor(() -> store).process(task);

            assertTrue(result.hasObjectReference());
            assertEquals(
                    TaskFlowObjectKeys.attemptOutputKey(
                            "job-video",
                            "task-video",
                            1,
                            assignmentId
                    ),
                    result.objectReference().key()
            );
            assertEquals(
                    result.objectReference(),
                    store.stat(result.objectReference().key())
            );
        } finally {
            System.clearProperty(PayloadLimits.MAX_INLINE_PAYLOAD_BYTES_PROPERTY);
        }
    }

    private void createVideoWithAudio(Path output) throws Exception {
        try (FFmpegFrameRecorder recorder = new FFmpegFrameRecorder(output.toString(), WIDTH, HEIGHT, 1)) {
            recorder.setFormat("mp4");
            recorder.setVideoCodec(AV_CODEC_ID_MPEG4);
            recorder.setPixelFormat(AV_PIX_FMT_YUV420P);
            recorder.setFrameRate(10);
            recorder.setGopSize(20);
            recorder.setSampleRate(SAMPLE_RATE);
            recorder.setAudioChannels(1);
            recorder.setAudioCodec(AV_CODEC_ID_AAC);
            recorder.setAudioBitrate(64_000);
            recorder.start();

            Java2DFrameConverter converter = new Java2DFrameConverter();
            BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_3BYTE_BGR);
            Graphics2D graphics = image.createGraphics();
            try {
                for (int frameIndex = 0; frameIndex < 8; frameIndex++) {
                    graphics.setColor(frameIndex % 2 == 0 ? Color.BLUE : Color.ORANGE);
                    graphics.fillRect(0, 0, WIDTH, HEIGHT);
                    recorder.record(converter.convert(image));
                    recorder.recordSamples(SAMPLE_RATE, 1, sineWave(frameIndex));
                }
            } finally {
                graphics.dispose();
            }
            recorder.stop();
        }
    }

    private ShortBuffer sineWave(int frameIndex) {
        short[] samples = new short[SAMPLES_PER_FRAME];
        int offset = frameIndex * SAMPLES_PER_FRAME;
        for (int i = 0; i < samples.length; i++) {
            double seconds = (offset + i) / (double) SAMPLE_RATE;
            samples[i] = (short) (Math.sin(2.0 * Math.PI * 440.0 * seconds) * Short.MAX_VALUE * 0.25);
        }
        return ShortBuffer.wrap(samples);
    }

    private boolean hasAudioSamples(Path video) throws Exception {
        try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(video.toFile())) {
            grabber.start();
            try {
                Frame frame;
                while ((frame = grabber.grab()) != null) {
                    if (frame.samples != null && frame.samples.length > 0) {
                        return true;
                    }
                }
                return false;
            } finally {
                grabber.stop();
            }
        }
    }
}
