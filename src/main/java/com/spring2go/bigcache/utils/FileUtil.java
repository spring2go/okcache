package com.spring2go.bigcache.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created on Jul, 2020 by @author bobo
 */
public class FileUtil {
    private static final int BUFFER_SIZE = 4096 * 4;

    /**
     * Only check if a given filename is valid according to the OS rules.
     *
     * You still need to handle other failures when actually creating
     * the file (e.g. insufficient permissions, lack of drive space, security restrictions).
     * @param file the name of a file
     * @return true if the file is valid, false otherwise
     */
    public static boolean isFilenameValid(String file) {
        File f = new File(file);
        try {
            f.getCanonicalPath();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        File[] subs = dir.listFiles();
        if (subs != null) {
            for (File f : dir.listFiles()) {
                if (f.isFile()) {
                    if(!f.delete()) {
                        throw new IllegalStateException("delete file failed: "+f);
                    }
                } else {
                    deleteDirectory(f);
                }
            }
        }
        if(!dir.delete()) {
            throw new IllegalStateException("delete directory failed: "+dir);
        }
    }

    public static void deleteFile(File file) {
        if (!file.exists() || !file.isFile()) {
            return;
        }
        if (!file.delete()) {
            throw new IllegalStateException("delete file failed: "+file);
        }
    }

    /**
     * Copy a directory and all of its contents.
     *
     * @param from source file
     * @param to target file
     * @return success or failure
     */
    public static boolean copyDirectory(File from, File to) {
        return copyDirectory(from, to, (byte[]) null);
    }

    public static boolean copyDirectory(String from, String to) {
        return copyDirectory(new File(from), new File(to));
    }

    public static boolean copyDirectory(File from, File to, byte[] buffer) {
        if (from == null) return false;
        if (!from.exists()) return true;
        if (!from.isDirectory()) return false;
        if (to.exists()) return false;
        if (!to.mkdirs()) return false;

        String[] list = from.list();
        // Some JVMs return null for File.list() when the directory is empty.
        if (list != null) {
            if (buffer == null) buffer = new byte[BUFFER_SIZE]; // return this buffer to copy files

            for(int i = 0; i < list.length; i++) {
                String fileName = list[i];

                File entry = new File(from, fileName);

                if (entry.isDirectory()) {
                    if (!copyDirectory(entry, new File(to, fileName), buffer)) {
                        return false;
                    }
                }
                else {
                    if (!copyFile(entry, new File(to, fileName), buffer)) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    public static boolean copyFile(File from, File to, byte[] buf) {
        if (buf == null) buf = new byte[BUFFER_SIZE];

        FileInputStream from_s = null;
        FileOutputStream to_s = null;

        try {
            from_s = new FileInputStream(from);
            to_s = new FileOutputStream(to);

            for(int bytesRead = from_s.read(buf); bytesRead > 0; bytesRead = from_s.read(buf)) {
                to_s.write(buf, 0, bytesRead);
            }

            to_s.getFD().sync();

        } catch (IOException ioe) {
            return false;
        } finally {
            if (from_s != null) {
                try {
                    from_s.close();
                    from_s = null;
                } catch (IOException ioe) {

                }
            }
            if (to_s != null) {
                try {
                    to_s.close();
                    to_s = null;
                } catch (IOException ioe) {
                }
            }
        }

        return true;
    }
}
