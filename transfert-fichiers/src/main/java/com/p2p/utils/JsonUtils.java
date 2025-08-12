package com.p2p.utils;

import com.google.gson.Gson;
import com.p2p.manager.Metadata;

import java.io.FileWriter;
import java.io.FileReader;

public class JsonUtils {
    private static final Gson gson = new Gson();

    public static void ecrireMetadata(Metadata metadata, String chemin) throws Exception {
        try (FileWriter writer = new FileWriter(chemin)) {
            gson.toJson(metadata, writer);
        }
    }

    public static Metadata lireMetadata(String chemin) throws Exception {
        try (FileReader reader = new FileReader(chemin)) {
            return gson.fromJson(reader, Metadata.class);
        }
    }
}
