package org.saliya.flinkit.utils;

import java.io.*;

public class SimpleBinaryWriter {
    public static void main(String[] args) {
        String file = "src/main/resources/sample.bin";
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file))) {
            int size = 8;
            int totalItems = size * size;
            for (int i = 0; i < size; ++i){
                for (int j = 0; j < size; ++j){
                    dos.writeShort((short)(((i*size+j)*1.0/totalItems)*Short.MAX_VALUE));
                }
            }
            dos.flush();
            dos.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
