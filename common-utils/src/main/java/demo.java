/**
 * @author yangshu06 <yangshu06@kuaishou.com>
 * Created on 2021-03-25
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

public class demo {
    public static void main(String[] args) {
        Configuration configuration = new HdfsConfiguration();
        configuration.addResource(demo.class.getClassLoader().getResource( "core-site.xml"));
        configuration.addResource(demo.class.getClassLoader().getResource( "hdfs-site.xml"));
        configuration.addResource(demo.class.getClassLoader().getResource("mountTable.xml"));
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            fileSystem.delete(new Path("/home"), true);

            Path path = new Path("/home");
            FileSystem fs = path.getFileSystem(configuration);

            OutputStream outputStream = fs.create(path);
            outputStream.write("this is a test".getBytes());
            outputStream.flush();
            outputStream.close();

            InputStream inputStream = fs.open(path);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                System.out.println(str);
            }
            inputStream.close();

            Trash.moveToAppropriateTrash(fs, path, configuration);
        } catch (IOException ioe) {
            System.out.println(ioe);
        }
    }
}
