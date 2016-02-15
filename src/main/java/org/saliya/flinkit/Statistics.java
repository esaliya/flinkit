package org.saliya.flinkit;

import akka.remote.security.provider.InternetSeedGenerator;
import edu.indiana.soic.spidal.common.DoubleStatistics;
import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.SerializedInputFormat;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.types.ShortValue;
import org.apache.hadoop.io.ShortWritable;
import org.saliya.flinkit.io.ShortMatrixInputFormat;

import java.nio.ByteOrder;
import java.nio.file.Paths;

public class Statistics {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);
        /*DataSet<String> text = env.readTextFile("src/main/resources/sample.txt");
        System.out.println(text.count());*/

//        String binaryFile = "src/main/resources/sample.bin";
        String binaryFile = args[0];
        /*SerializedInputFormat<ShortValue> sif = new SerializedInputFormat<>();
        sif.setFilePath(binaryFile);
        DataSet<ShortValue> ds = env.createInput(sif,
            ValueTypeInfo.SHORT_VALUE_TYPE_INFO);
        System.out.println(ds.count());*/

        int globalColCount = Integer.parseInt(args[1]);
        boolean isBigEndian = Boolean.parseBoolean(args[2]);
        boolean divideByShortMax = Boolean.parseBoolean(args[3]);
        double factor = divideByShortMax ? 1.0/Short.MAX_VALUE : 1.0;
        ShortMatrixInputFormat smif = new ShortMatrixInputFormat();
        smif.setFilePath(binaryFile);
        smif.setBigEndian(isBigEndian);
        smif.setGlobalColumnCount(globalColCount);
        DataSet<Short[]> ds = env.createInput(smif, BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO);
        MapOperator<Short[], DoubleStatistics> op = ds.map(arr -> {

            DoubleStatistics stats = new DoubleStatistics();
            for (int i = 0; i < arr.length; ++i){
                stats.accept(arr[i]*factor);
            }

            /*for (int i = 0; i < arr.length; ++i){
                System.out.print(arr[i] + " ");
                if ((i+1)%globalColCount == 0){
                    System.out.println();
                }
            }*/
            return stats;
        });
        op.reduce((a,b)-> {a.combine(b); return a;}).print();
//        System.out.println(ds.count());
    }
}
