package org.saliya.flinkit;

import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.SerializedInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.types.ShortValue;
import org.apache.hadoop.io.ShortWritable;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("src/main/resources/sample.txt");
        System.out.println(text.count());

        String binaryFile = "src/main/resources/sample.bin";
        SerializedInputFormat<ShortValue> sif = new SerializedInputFormat<>();
        sif.setFilePath(binaryFile);
        DataSet<ShortValue> ds = env.createInput(sif,
            ValueTypeInfo.SHORT_VALUE_TYPE_INFO);
        System.out.println(ds.count());
    }
}
