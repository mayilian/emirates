import MonitoringThreads.ArchiveRunnable;
import MonitoringThreads.EmailRunnable;
import MonitoringThreads.ImagesRunnable;
import MonitoringThreads.TextRunnable;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.file.*;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// .zip, .jpg, .png, .csv, .doc, .pdf, .xml, .eml
public class DirectoryWatcher {
    private final static Logger logger = LogManager.getLogger(DirectoryWatcher.class);

    private static void usage() {
        System.err.println("usage: java DirectoryWatcher [-r] dir");
        System.exit(-1);
    }

    public static void main(String[] args)  {
        BasicConfigurator.configure();

        if (args.length == 0 || args.length > 2) {
            usage();
        }

        Path dir = Paths.get(args[0]);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            executor.submit(new ArchiveRunnable(dir));
            executor.submit(new ImagesRunnable(dir));
            executor.submit(new TextRunnable(dir));
            executor.submit(new EmailRunnable(dir));
        } catch (IOException e) {
            logger.error("Failed to submit task", e);
        }

        executor.shutdown();
    }
}
