package org.example.file;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class FileMonitor {
    @ConfigProperty(name = "file.monitor.rate", defaultValue = "5s")
    String monitorRate;
    @ConfigProperty(name = "file.monitor.path")
    Path streamsDataPath;
    Path indexPath;

    @PostConstruct
    void init() {
        // Fail to initialize the indexPath
        try {
            if (Files.exists(streamsDataPath)) {
                Files.move(streamsDataPath, streamsDataPath.resolveSibling(streamsDataPath.getFileName() + ".old"), StandardCopyOption.REPLACE_EXISTING);
                Log.info("Successfully moved old streams data to " + streamsDataPath+".old");
            }
            Files.createFile(streamsDataPath);
        }  catch (IOException e) {
            Log.errorf(e, "Failed to create file %s", streamsDataPath);
        }
        Log.infof("Initializing FileMonitor, streamsDataPath=%s, indexPath=%s", streamsDataPath, indexPath);
    }

    @Scheduled(identity = "FileMonitor", every = "${file.monitor.rate:5s}", delay = 10, delayUnit = TimeUnit.SECONDS)
    @RunOnVirtualThread
    public void processBatch() {
        // Use indexPath
        try (FileChannel streamsChannel = FileChannel.open(streamsDataPath, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
             FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
        ) {
            // ...
        } catch (IOException e) {
            Log.errorf(e, "Failed checking data for: %s", streamsDataPath);
        }
    }
}
