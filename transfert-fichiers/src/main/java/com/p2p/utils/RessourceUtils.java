package com.p2p.utils;

import java.io.File;

public class RessourceUtils {

    public static String getCheminBase() {
        File uploads = new File("./uploads");
        if (!uploads.exists()) {
            uploads.mkdirs();
        }
        return uploads.getAbsolutePath();
    }
}
