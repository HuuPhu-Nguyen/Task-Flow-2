package peer.processors;

import protocol.PayloadLimits;
import com.google.gson.Gson;
import conversion.model.FilePayload;
import objectstore.ObjectStoreProvider;
import objectstore.ObjectStores;
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
import java.util.Locale;
import java.util.Objects;

public class ImageConversionProcessor implements TaskProcessor<FilePayload> {
    private final Gson gson = new Gson();
    private final ObjectStoreProvider objectStoreProvider;

    public ImageConversionProcessor() {
        this(ObjectStores::open);
    }

    public ImageConversionProcessor(ObjectStoreProvider objectStoreProvider) {
        this.objectStoreProvider = Objects.requireNonNull(objectStoreProvider, "objectStoreProvider");
    }

    @Override
    public FilePayload process(TaskAssignMessage task) throws Exception {
        String format = normalizeFormat(task.getParam());
        FilePayload input = gson.fromJson(gson.toJson(task.getPayload()), FilePayload.class);
        byte[] rawBytes = ObjectBackedPayloadReader.readInput(
                input,
                "Image",
                objectStoreProvider
        );
        String inputFileName = SafeFileNames.sanitize(input.fileName());
        BufferedImage img = decode(rawBytes, inputFileName);

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

        String contentType = format.equals("jpg") ? "image/jpeg" : "image/" + format;
        return ConversionOutputPublisher.publish(
                task,
                newFileName,
                contentType,
                outputBytes,
                objectStoreProvider
        );
    }

    BufferedImage decode(byte[] rawBytes, String inputFileName) throws IOException {
        if (inputFileName.toLowerCase(Locale.ROOT).endsWith(".pdf")) {
            try (PDDocument document = Loader.loadPDF(rawBytes)) {
                PDFRenderer renderer = new PDFRenderer(document);
                return renderer.renderImageWithDPI(0, 300);
            }
        }
        return ImageIO.read(new ByteArrayInputStream(rawBytes));
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
