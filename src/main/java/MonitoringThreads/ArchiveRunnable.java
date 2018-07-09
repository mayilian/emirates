package MonitoringThreads;


import ESTransport.ESClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.pkg.PackageParser;
import org.apache.tika.sax.BodyContentHandler;
import org.elasticsearch.action.index.IndexResponse;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import parser.FilesParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ArchiveRunnable extends BaseRunnable {
    private final static Logger logger = LogManager.getLogger(ArchiveRunnable.class);
    private final static String ARCHIVE_DIR = "archive";

    public ArchiveRunnable(Path dir) throws IOException {
        super(dir.resolve(ARCHIVE_DIR));
    }

    void indexFileContent(Path fileToIndex) {
        Parser parser = new FilesParser(new PackageParser());
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
            IndexResponse response = ESClient.INSTANCE.getClient().prepareIndex(ARCHIVE_DIR, "_doc", getFileRelativeName(fileToIndex))
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
        return ARCHIVE_DIR;
    }
}
