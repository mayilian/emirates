package parser;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by david on 7/7/18.
 */
public class FilesParser extends ParserDecorator {
    public FilesParser(Parser parser) {
        super(parser);
    }

    @Override
    public void parse(
            InputStream stream, ContentHandler content,
            Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        super.parse(stream, content, metadata, context);
    }
}
