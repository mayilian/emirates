package MonitoringThreads;

import org.apache.commons.io.IOUtils;
import org.apache.james.mime4j.MimeException;
import org.apache.james.mime4j.codec.DecodeMonitor;
import org.apache.james.mime4j.message.DefaultBodyDescriptorBuilder;
import org.apache.james.mime4j.parser.ContentHandler;
import org.apache.james.mime4j.parser.MimeStreamParser;
import org.apache.james.mime4j.stream.BodyDescriptorBuilder;
import org.apache.james.mime4j.stream.MimeConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import tech.blueglacier.email.Attachment;
import tech.blueglacier.email.Email;
import tech.blueglacier.parser.CustomContentHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EmailRunnable extends BaseRunnable {
    private final static Logger logger = LogManager.getLogger(EmailRunnable.class);
    private final static String EMAIL_DIR = "emails";

    /**
     * Creates a WatchService and registers the given directory
     *
     * @param dir
     */
    public EmailRunnable(Path dir) throws IOException {
        super(dir.resolve(dir.resolve(EMAIL_DIR)));
    }

    @Override
    void indexFileContent(Path fileToIndex) {
        ContentHandler contentHandler = new CustomContentHandler();

        MimeConfig mime4jParserConfig = MimeConfig.DEFAULT;
        BodyDescriptorBuilder bodyDescriptorBuilder = new DefaultBodyDescriptorBuilder();
        MimeStreamParser mime4jParser = new MimeStreamParser(mime4jParserConfig,  DecodeMonitor.SILENT,bodyDescriptorBuilder);
        mime4jParser.setContentDecoding(true);
        mime4jParser.setContentHandler(contentHandler);

        try {
            mime4jParser.parse(new FileInputStream(fileToIndex.toString()));
        } catch (MimeException | IOException e) {
            logger.error("Could not extract content from file: " + fileToIndex, e);

            storeFailedFile(fileToIndex);
            return;
        }

        Email email = ((CustomContentHandler) contentHandler).getEmail();


        Attachment plainText = email.getPlainTextEmailBody();
        String plainTextString = email.toString();
        if (plainText != null) {
            StringWriter plainTextWriter = new StringWriter();
            try {
                IOUtils.copy(plainText.getIs(), plainTextWriter, "UTF-8");
                plainTextString = plainTextWriter.toString();
            } catch (IOException e) {
                logger.warn("Couldn't extract plainText from email");
            }
        }

        String to = email.getToEmailHeaderValue();
        String cc = email.getCCEmailHeaderValue();
        String from = email.getFromEmailHeaderValue();


        List<Attachment> attachments =  email.getAttachments();
        StringBuilder stringBuilder = new StringBuilder();
        for (Attachment attachment : attachments) {
            stringBuilder.append(attachment.getAttachmentName()).append(",");
        }

        if (attachments.size() > 0) {
            stringBuilder.setLength(stringBuilder.length() - 1);
        }
        String attachmentNames = stringBuilder.toString();

        try {
            IndexResponse response = ESTransport.ESClient.INSTANCE.getClient().prepareIndex(EMAIL_DIR, "_doc", getFileRelativeName(fileToIndex))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("to", to)
                            .field("cc", cc)
                            .field("from", from)
                            .field("plainText", plainTextString)
                            .field("attachments", attachmentNames)
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
        return EMAIL_DIR;
    }
}
