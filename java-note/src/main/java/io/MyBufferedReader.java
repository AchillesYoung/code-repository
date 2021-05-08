package io;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;


public class MyBufferedReader extends Reader {

    Reader in;

    public MyBufferedReader(Reader in) {
        this.in=in;
    }
    /**
     * read line
     * @return
     * @throws IOException
     */

    public String myReadLine()  throws IOException{
        int data;
        StringBuilder builder  = new StringBuilder();
        while((data = in.read())!= -1){
            //\r is a char?
            if((char)data=='\r'){
               continue;
            }
            //
            if((char)data=='\n'){
                return builder.toString();
            }
            builder.append((char)data);
        }
        return null;
    }


    /**
     * Reads characters into a portion of an array.  This method will block
     * until some input is available, an I/O error occurs, or the end of the
     * stream is reached.
     *
     * @param cbuf Destination buffer
     * @param off  Offset at which to start storing characters
     * @param len  Maximum number of characters to read
     * @return The number of characters read, or -1 if the end of the
     * stream has been reached
     * @throws IOException If an I/O error occurs
     */
    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return 0;
    }

    /**
     * Closes the stream and releases any system resources associated with
     * it.  Once the stream has been closed, further read(), ready(),
     * mark(), reset(), or skip() invocations will throw an IOException.
     * Closing a previously closed stream has no effect.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void close() throws IOException {

    }
    public static void main(String[] args) throws IOException {


        MyBufferedReader br = new MyBufferedReader(new FileReader("测试题19.txt"));

        String line ;
        while((line=br.myReadLine())!=null){
            System.out.println(line);
        }

        br.close();
    }
}
