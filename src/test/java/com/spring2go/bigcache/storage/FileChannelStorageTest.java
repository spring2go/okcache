package com.spring2go.bigcache.storage;

import com.spring2go.bigcache.utils.FileUtil;
import com.spring2go.bigcache.utils.TestUtil;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class FileChannelStorageTest {
    private static String testDir = TestUtil.TEST_BASE_DIR + "unit/file_channel_storage_test/";

    private IStorage fileChannelStorage = null;

    @Test
    public void testFileChannelStorage() throws IOException {
        fileChannelStorage = new FileChannelStorage(testDir, 1, 16 * 1024 * 1024);

        // test one item
        String testString = "Test String";
        fileChannelStorage.put(0, testString.getBytes());
        byte[] dest = new byte[testString.getBytes().length];
        fileChannelStorage.get(0, dest);

        assertEquals(testString, new String(dest));

        // test limit items
        int limit = 1000;
        int[] positionArray = new int[limit];
        int[] lengthArray = new int[limit];
        int position = 0;
        for(int i = 0; i < limit; i++) {
            byte[] src = (testString + i).getBytes();
            positionArray[i] = position;
            lengthArray[i] = src.length;
            fileChannelStorage.put(position, src);
            position += src.length;
        }

        for(int i = 0; i < limit; i++) {
            dest = new byte[lengthArray[i]];
            fileChannelStorage.get(positionArray[i], dest);
            assertEquals(testString + i, new String(dest));
        }
    }

    @After
    public void clear() throws IOException {
        if (this.fileChannelStorage != null) {
            this.fileChannelStorage.close();
        }
        FileUtil.deleteDirectory(new File(testDir));
    }
}
