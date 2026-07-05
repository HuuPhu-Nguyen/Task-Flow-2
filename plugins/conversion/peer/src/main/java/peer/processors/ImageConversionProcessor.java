package peer.processors;

import protocol.LocalPayloadStorage;
import protocol.PayloadLimits;
import com.google.gson.Gson;
import conversion.model.FilePayload;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import peer.engine.TaskProcessor;
import protocol.SafeFileNames;
import protocol.TaskAssignMessage;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Locale;

public class ImageConversionProcessor implements TaskProcessor<FilePayload> {
    private final Gson gson = new Gson();

    @Override
    public FilePayload process(TaskAssignMessage task) throws Exception {
        String format = normalizeFormat(task.getParam());
        FilePayload input = gson.fromJson(gson.toJson(task.getPayload()), FilePayload.class);
        long maxInputBytes = PayloadLimits.maxInputBytes();
        byte[] rawBytes = readPayloadBytes(input, maxInputBytes, "Image task has no input data.");
        BufferedImage img;
        String inputFileName = SafeFileNames.sanitize(input.fileName());
        if (inputFileName.toLowerCase(Locale.ROOT).endsWith(".pdf")) {
            try (PDDocument document = Loader.loadPDF(rawBytes)) {
                PDFRenderer renderer = new PDFRenderer(document);
                img = renderer.renderImageWithDPI(0, 300);
            }
        } else {
            img = ImageIO.read(new ByteArrayInputStream(rawBytes));
        }

        if (img == null) {
            throw new IOException("Could not decode data for " + input.fileName());
        }

        if (format.equals("jpg") || format.equals("jpeg")) {
            BufferedImage rgbImage = new BufferedImage(
                    img.getWidth(), img.getHeight(), BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = rgbImage.createGraphics();
            g2d.setColor(Color.WHITE);
            g2d.fillRect(0, 0, img.getWidth(), img.getHeight());
            g2d.drawImage(img, 0, 0, null);
            g2d.dispose();
            img = rgbImage;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (!ImageIO.write(img, format, baos)) {
            throw new IOException("No ImageIO writer found for target format: " + format);
        }
        if (baos.size() == 0) {
            throw new IOException("Conversion produced no output for " + input.fileName());
        }
        long maxResultBytes = PayloadLimits.maxResultBytes();
        if (baos.size() > maxResultBytes) {
            throw new IOException("Conversion result exceeds " + PayloadLimits.MAX_RESULT_BYTES_ENV
                    + " (" + maxResultBytes + " bytes): " + input.fileName());
        }

        byte[] outputBytes = baos.toByteArray();
        String newFileName = stripExtension(inputFileName) + "." + format;

        return outputPayload(newFileName, outputBytes);
    }

    private byte[] readPayloadBytes(FilePayload payload, long maxBytes, String emptyMessage) throws IOException {
        if (payload == null || (!payload.hasInlineData() && !payload.hasPayloadReference())) {
            throw new IOException(emptyMessage);
        }
        if (payload.hasInlineData() == payload.hasPayloadReference()) {
            throw new IOException("Image task must contain exactly one of Base64 data or a payload reference: "
                    + payload.fileName());
        }
        if (payload.hasPayloadReference()) {
            return LocalPayloadStorage.read(payload.payloadReference(), maxBytes);
        }
        byte[] rawBytes;
        try {
            rawBytes = Base64.getDecoder().decode(payload.base64Data());
        } catch (IllegalArgumentException e) {
            throw new IOException("Image task payload is not valid Base64: " + payload.fileName(), e);
        }
        if (rawBytes.length > maxBytes) {
            throw new IOException("Input payload exceeds " + PayloadLimits.MAX_INPUT_BYTES_ENV
                    + " (" + maxBytes + " bytes): " + payload.fileName());
        }
        return rawBytes;
    }

    private FilePayload outputPayload(String fileName, byte[] bytes) throws IOException {
        if (LocalPayloadStorage.shouldExternalize(bytes.length)) {
            return new FilePayload(fileName, null, LocalPayloadStorage.storeBytes(fileName, bytes));
        }
        return new FilePayload(fileName, Base64.getEncoder().encodeToString(bytes));
    }

    private String normalizeFormat(String format) throws IOException {
        if (format == null || format.isBlank()) {
            throw new IOException("Image conversion target format is required.");
        }
        String normalized = format.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("jpeg") ? "jpg" : normalized;
    }

    private String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return (dot == -1) ? fileName : fileName.substring(0, dot);
    }
}
