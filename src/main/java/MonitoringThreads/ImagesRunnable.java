package MonitoringThreads;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.jpeg.JpegParser;
import org.apache.tika.sax.BodyContentHandler;
import org.elasticsearch.action.index.IndexResponse;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import parser.FilesParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

//supports only jpeg
public class ImagesRunnable extends BaseRunnable implements Runnable {
    private final static Logger logger = LogManager.getLogger(ImagesRunnable.class);
    private final static String IMAGES_DIR = "images";

    /**
     * Creates a WatchService and registers the given directory
     *
     * @param dir
     */
    public ImagesRunnable(Path dir) throws IOException {
        super(dir.resolve(dir.resolve(IMAGES_DIR)));
    }

    @Override
    void indexFileContent(Path fileToIndex) {
        Parser parser = new FilesParser(new JpegParser());
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);

        ContentHandler contentHandler = new BodyContentHandler();
        Metadata metadata = new Metadata();

        try(InputStream stream = TikaInputStream.get(fileToIndex)) {
            parser.parse(stream, contentHandler, metadata, context);
        } catch (IOException | TikaException | SAXException e) {
            logger.error("Could not extract content from file: " + fileToIndex, e);

            storeFailedFile(fileToIndex);
            return;
        }

        try {
            IndexResponse response = ESTransport.ESClient.INSTANCE.getClient().prepareIndex(IMAGES_DIR, "_doc", getFileRelativeName(fileToIndex))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("content", contentHandler.toString())
                            .field("metadata", metadata.toString())
                            .endObject()).get();

            if (logger.isInfoEnabled()) {
                logger.info("Indexed file: " + getFileRelativeName(fileToIndex) + ", index ID=" + response.getId());
            }
        } catch (IOException e) {
            logger.error("Could not index archive file : " + getFileRelativeName(fileToIndex), e);
        }
    }

    @Override
    String getFolderName() {
        return IMAGES_DIR;
    }
}
