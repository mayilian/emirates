package MonitoringThreads;

import ESTransport.ESClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.util.TextUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

abstract class BaseRunnable implements Runnable {
    private final static Logger logger = LogManager.getLogger(BaseRunnable.class);

    private final WatchService watcher;
    private final Map<WatchKey,Path> keys;
    private boolean trace = false;
    private final String processedFolder = System.getProperty("user.dir") + File.separator + getFolderName();
    private final File processedFolderDir = new File(processedFolder);
    private final BlockingQueue<Path /*file paths to be processed*/> filePathsQueue = new LinkedBlockingDeque<>();

    @SuppressWarnings("unchecked")
    private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>)event;
    }

    /**
     * Register the given directory with the WatchService
     */
    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE);
        if (trace) {
            Path prev = keys.get(key);
            if (prev == null) {
                System.out.format("register: %s\n", dir);
            } else {
                if (!dir.equals(prev)) {
                    System.out.format("update: %s -> %s\n", prev, dir);
                }
            }
        }
        keys.put(key, dir);
    }

    /**
     * Creates a WatchService and registers the given directory
     */
    BaseRunnable(Path dir) throws IOException {

        // add the files to the queue so that they can be later processed
        Files.walk(dir).filter(Files::isRegularFile).forEach(this::storeFileAndQueueFilePath);

        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<>();

        register(dir);

        // enable trace after initial registration
        this.trace = true;

        // create corresponding processed files folder
        if (!processedFolderDir.exists())  {
            if (processedFolderDir.mkdir()) {
                logger.debug("Created folder for processed files: " + processedFolder);
            } else {
                logger.debug("Failed to create folder for processed files: " + processedFolder);
            }
        }

        startIndexing();
    }

    /**
     * Process all events for keys queued to the watcher
     */
    private void processEvents() throws IOException {
        for (;;) {
            // wait for key to be signalled
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                System.err.println("WatchKey not recognized!!");
                continue;
            }

            for (WatchEvent<?> event: key.pollEvents()) {
                WatchEvent.Kind kind = event.kind();

                if (kind == OVERFLOW) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);

                storeFileAndQueueFilePath(child);
            }

            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);

                // all directories are inaccessible
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }

    void storeFailedFile(Path failedFile) {
        if (logger.isInfoEnabled()) {
            logger.info("Storing failed file:" + failedFile.getFileName().toString());
        }

        try {
            IndexResponse response = ESClient.INSTANCE.getClient().prepareIndex("failed", "_doc", getFileRelativeName(failedFile))
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("content", "failed")
                            .endObject()).get();

            if (logger.isInfoEnabled()) {
                logger.info("Indexed failed file: " + failedFile.getFileName().toString() + ", index ID=" + response.getId());
            }
        } catch (IOException e) {
            logger.error("Could not index failed file : " + failedFile.getFileName().toString(), e);
        }

    }

    private void startIndexing() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            while (true) {
                try {
                    Path path = filePathsQueue.take();
                    indexFileContent(path);

                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    logger.warn("Interrupted while taking a path from queue");
                }
            }
        });

        executorService.shutdown();
    }

    abstract void indexFileContent(Path fileToBeIndexed);

    abstract String getFolderName();

    String getFileRelativeName(Path file) {
        int nameCount = file.getNameCount();
        return file.getName(nameCount - 2) + File.separator + file.getName(nameCount - 1);
    }

    @Override
    public void run() {
        try {
            processEvents();
        } catch (IOException e) {
            logger.error("Could not process file", e);
        }
    }

    private Path storeFile(Path file) throws IOException {
        //storing the file
        String fileName = file.getFileName().toString();
        File destFile = new File(processedFolderDir, fileName);
        if (destFile.exists()) {
            boolean sameContent = FileUtils.contentEquals(file.toFile(), destFile);
            if (sameContent) {
                Files.delete(file);
                return null;
            } else {
                String fileNameWithOutExt = FilenameUtils.removeExtension(fileName);
                String ext = FilenameUtils.getExtension(fileName);
                if (!TextUtils.isEmpty(ext)) {
                    ext = "." + ext;
                }
                fileName = fileNameWithOutExt + "_" + UUID.randomUUID() + ext;
                destFile = new File(processedFolderDir, fileName);
            }
        }

        Path dest = destFile.toPath();
        Files.move(file, destFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

        return dest;
    }

    private void storeFileAndQueueFilePath(Path filePath) {
        try {
            Path path = storeFile(filePath);
            if (path != null) {
                filePathsQueue.offer(path);
            } else {
                logger.warn("Do not store, the path is null. This usually happens when the file is already processed once");
            }
        } catch (IOException e) {
            logger.error("Couldn't store the file: " + filePath);
        }
    }
}
