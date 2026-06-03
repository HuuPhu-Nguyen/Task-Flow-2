package peer.processors;

import com.google.gson.Gson;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import peer.engine.TaskProcessor;
import protocol.FilePayload;
import protocol.TaskAssignMessage;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Base64;

public class ImageConversionProcessor implements TaskProcessor<FilePayload> {
    private final Gson gson = new Gson();

    @Override
    public FilePayload process(TaskAssignMessage task) throws Exception {
        String format = task.getParam();
        FilePayload input = gson.fromJson(gson.toJson(task.getPayload()), FilePayload.class);
        byte[] rawBytes = Base64.getDecoder().decode(input.base64Data());
        BufferedImage img;
        // Check if the input is a PDF
        if (input.fileName().toLowerCase().endsWith(".pdf")) {
            try (PDDocument document = Loader.loadPDF(rawBytes)) {
                PDFRenderer renderer = new PDFRenderer(document);
                // Render the first page at a standard 300 DPI for high quality
                img = renderer.renderImageWithDPI(0, 300);
            }
        } else {
            // Standard ImageIO path for PNG/JPG/etc.
            img = ImageIO.read(new ByteArrayInputStream(rawBytes));
        }

        if (img == null) {
            throw new IOException("Could not decode data for " + input.fileName());
        }

        // Handle transparency for JPEG targets
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

        String outBase64 = Base64.getEncoder().encodeToString(baos.toByteArray());
        String newFileName = stripExtension(input.fileName()) + "." + format;

        return new FilePayload(newFileName, outBase64);
    }

    private String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return (dot == -1) ? fileName : fileName.substring(0, dot);
    }
}
