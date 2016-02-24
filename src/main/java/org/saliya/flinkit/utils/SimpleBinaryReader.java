package org.saliya.flinkit.utils;

import java.io.*;

public class SimpleBinaryReader {
    public static void main(String[] args) {
        String file = "src/main/resources/sample.bin";
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            int size = 128;
            for (int i = 0; i < size; ++i){
                for (int j = 0; j < size; ++j){
                    System.out.print(dis.readShort() + " ");
                }
                System.out.println();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
