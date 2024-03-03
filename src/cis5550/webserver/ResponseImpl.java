package cis5550.webserver;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ResponseImpl implements Response{
    byte[] bodyRaw = null;
    int m_statusCode = 200;
    String m_reasonPhrase = "OK";
    OutputStream m_out;
    Map<String,String> headers;

    boolean hasCommited = false;
    boolean hasClosed = false;

    Server server;

    ResponseImpl(Server serverArg, OutputStream out)
    {
        server = serverArg;
        m_out = out;
        headers = new HashMap<>();

    }


    public void body(String body)
    {
        if(!hasCommited)
        {
            bodyRaw = body.getBytes();
        }
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        bodyRaw = bodyArg.clone();
    }

    @Override
    public void header(String name, String value) {
        headers.put(name.toLowerCase(),value);
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        //TODO: extra credit phase
        status(statusCode,reasonPhrase);
        headers.put("content-length",""+0);
        try {
            sendHeader();
            if(!hasClosed)
            {
                hasClosed = true;
                hasCommited = true;
            }
        }
        catch (Exception e)
        {
            System.out.println("error when halting:"+e);
        }
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if(!hasCommited)
        {
            m_statusCode = statusCode;
            m_reasonPhrase = reasonPhrase;
        }
    }

    @Override
    public void type(String contentType) {
        header("Content-Type",contentType);
    }

    public void sendHeader() throws IOException {
        m_out.write(getHeaderContent().getBytes());
    }

    // write is not for write to output stream but to write into body.
    @Override
    public void write(byte[] b) throws Exception {
        if(bodyRaw != null)
        {
            headers.put("content-length",""+(bodyRaw.length+b.length));
            byte[] newBody = new byte[bodyRaw.length+b.length];
            System.arraycopy(bodyRaw,0,newBody,0,bodyRaw.length);
            System.arraycopy(b,0,newBody,bodyRaw.length,b.length);
//            System.out.println("before write body:"+new String(bodyRaw, StandardCharsets.UTF_8));
//            System.out.println("size change from"+bodyRaw.length+" to "+(bodyRaw.length+b.length));
            bodyRaw = newBody.clone();
//            System.out.println("after write body:"+new String(bodyRaw, StandardCharsets.UTF_8));
        }
        else
        {
            headers.put("content-length",""+b.length);
            bodyRaw = b.clone();
        }

//        m_out.write(headerBytes);
//        m_out.write(bodyRaw);
    }

    public String getHeaderContent()
    {
        String headerContent = "HTTP/1.1 " + m_statusCode + " " + m_reasonPhrase + CRLF;

        if(headers.containsKey("content-type"))
            headerContent += "Content-Type: "+ headers.get("content-type") + CRLF;
        else
            headerContent += "Content-Type: text/html" + CRLF;
        if(bodyRaw != null)
            headerContent += "Content-Length: " + bodyRaw.length + CRLF;
        headerContent += "Server: server" + CRLF;
        if(headers != null)
        {
            for(var entry: headers.entrySet())
            {
                headerContent += entry.getKey()+": "+entry.getValue() + CRLF;
            }
        }
        else
        {
            System.out.println("headers is null");
        }
        headerContent += CRLF; // to ensure double CRLF

        return headerContent;
    }

    public static final String CRLF = "\r\n";
}
