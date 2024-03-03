package cis5550.webserver;

import cis5550.tools.SNIInspector;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {
    static Server staticServer = null;
    static Thread requestThread = null;
    static Thread secureRequestThread = null;

// use serversocket.getLocalport to get port num
    HashMap<String, String> sessionArgs = new HashMap<>();
    HashMap<String, String> m_returnDir = new HashMap<>();
    ServerSocket m_serverSocket;
    ServerSocket m_secureServerSocket;

    static String currHost = null;
    static HashMap<String, AbstractMap.SimpleImmutableEntry<String,String>> hostKeyPwd;
    static public final String DEFAULT = "localhost";

    static public HashMap<String, SessionImpl> sessionMap = new HashMap<>();

    static HashMap<String,HashMap<String, Route>>  getHostMap = new HashMap<>();
    static HashMap<String,HashMap<String, Route>>  putHostMap = new HashMap<>();
    static HashMap<String,HashMap<String, Route>>  postHostMap = new HashMap<>();

    static Route beforeRoute;
    static Route afterRoute;

    public static class staticFiles
    {
        public static void location(String P) {
            if(staticServer == null)
            {
                staticServer = new Server();
                staticServer.runOnPort(80);
            }
            if(currHost == null)
            {
                staticServer.m_returnDir.put(DEFAULT,P);
            }
            else
            {
                staticServer.m_returnDir.put(currHost,P);
            }
        }
    }

    public static void tryInitialServer() {
        if(staticServer == null)
        {
            staticServer = new Server();
            staticServer.runOnPort(80);
        }
        if(requestThread == null )
        {
            if(staticServer.m_serverSocket == null)
                staticServer.runOnPort(80);
            requestThread = new Thread(Server::run);
            requestThread.start();
        }
    }

    public static void host(String H,String key, String pwd)
    {
        currHost = H;
        hostKeyPwd.put(H,new AbstractMap.SimpleImmutableEntry<>(key,pwd));
    }

    public static void before(Route lambda)
    {
        beforeRoute = lambda;
    }

    public static void after(Route lambda)
    {
        afterRoute = lambda;
    }

    public static String getError(int statusCode)
    {
        switch (statusCode)
        {
            case 200:
                return " OK";
            case 304:
                return Error304;
            case 400:
                return Error400;
            case 403:
                return Error403;
            case 404:
                return Error404;
            case 405:
                return Error405;
            case 501:
                return Error501;
            case 505:
                return Error505;
            default:
                return "OK";
        }
    }

    public static void sendResponse(OutputStream out,
                                    int statusCode,
                                    String contentType,
                                    File fileInput,
                                    InputStream fileInputStream,
                                    boolean onlyHeader) {
        String responseMessage = "HTTP/1.1 "+statusCode+getError(statusCode)+CRLF;
        //TODO: change OK to other things
        if(fileInputStream != null)
        {
            try {
                contentType = Files.probeContentType(fileInput.toPath());
            }
            catch (Exception e)
            {
                System.out.println("fail to probe content type");
                e.printStackTrace();
            }
        }
        responseMessage += "Content-Type: "+contentType+CRLF;
        responseMessage += "Server: myServer"+CRLF;
        responseMessage += "Content-Length: ";

        if(fileInputStream != null)
        {
            try {
                byte[] byteBuffer = new byte[100000];
                int size = 0;
                int tempChar = fileInputStream.read();
                while(tempChar!=-1)
                {
                    byteBuffer[size++] = (byte) tempChar;
                    tempChar = fileInputStream.read();
                }
                responseMessage += size + CRLF + CRLF;
                out.write(responseMessage.getBytes());
                if(!onlyHeader)
                {
                    out.write(byteBuffer,0,size);
                }
            }
            catch (IOException e)
            {
                System.out.println("incur exception when reading");
                e.printStackTrace();
            }
        }
        else    //error case
        {
            responseMessage += 0 + CRLF + CRLF;
            try {
                out.write(responseMessage.getBytes());
            }
            catch (IOException e)
            {
                System.out.println("fail to write to response");
            }
        }

    }

    void runOnPort(int N) {
        try {
            m_serverSocket = new ServerSocket(N);
        }
        catch (Exception e)
        {
            System.out.println("fail to run on port "+N);
            e.printStackTrace();
        }
    }

    public static void port(int N) {
        if(staticServer == null)
        {
            staticServer = new Server();
        }
        staticServer.runOnPort(N);
    }

    public static void get(String P, Route route) {
        tryInitialServer();
        if(currHost == null) {
            if(!getHostMap.containsKey(DEFAULT))
            {
                getHostMap.put(DEFAULT,new HashMap<>());
            }
            getHostMap.get(DEFAULT).put(P, route);
        }
        else
        {
            if(!getHostMap.containsKey(currHost))
            {
                getHostMap.put(currHost,new HashMap<>());
            }
            getHostMap.get(currHost).put(P,route);
        }
        //TODO: should create GET, PUT, and POST routes, respectively;
        //p is a path pattern (String), and L is a lambda that accepts a pair (req,res), where req is an
        //object of type Request and res is an object of type Response, and returns an Object.
    }

    public static void post(String P, Route route) {
        tryInitialServer();
        if(currHost == null) {
            if(!postHostMap.containsKey(DEFAULT))
            {
                postHostMap.put(DEFAULT,new HashMap<>());
            }
            postHostMap.get(DEFAULT).put(P, route);
        }
        else
        {
            if(!postHostMap.containsKey(currHost))
            {
                postHostMap.put(currHost,new HashMap<>());
            }
            postHostMap.get(currHost).put(P,route);
        }
    }

    public static void put(String P, Route route) {
        tryInitialServer();
        if(currHost == null) {
            if(!putHostMap.containsKey(DEFAULT))
            {
                putHostMap.put(DEFAULT,new HashMap<>());
            }
            putHostMap.get(DEFAULT).put(P, route);
        }
        else
        {
            if(!putHostMap.containsKey(currHost))
            {
                putHostMap.put(currHost,new HashMap<>());
            }
            putHostMap.get(currHost).put(P,route);
        }
    }

    public static void secureServerLoop()
    {
        ArrayList<RequestHandler> requestHandlers = new ArrayList<RequestHandler>();
        ArrayList<Socket> clientSocketList = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(100);

        while(true) {
            try {
                clientSocketList.add(staticServer.m_secureServerSocket.accept());
                // TODO: modify the requesthandler to eliminate shared server info.
                requestHandlers.add(new RequestHandler(staticServer,
                        clientSocketList.get(clientSocketList.size()-1)));
                requestHandlers.get(requestHandlers.size()-1).isSecure = true;
                executor.execute(requestHandlers.get(requestHandlers.size()-1));
//                requestHandlers.get(requestHandlers.size()-1).start();
            } catch (Exception e) {
                System.out.println("into e "+e);
                throw new RuntimeException(e);
            }
        }
    }

//    public static void main(String[] args) {
////        port(8443);
//        securePort(443);
//        get("/", (req,res) -> { return "Hello World - this is David Tian"; });
//    }

    public static void securePort(int portNum) {
        String pwd = "secret";
        KeyStore keyStore = null;
        if(staticServer == null)
        {
            staticServer = new Server();
        }

        try {
            keyStore = KeyStore.getInstance("JKS");
        }
        catch (KeyStoreException e)
        {
            System.out.println("error when loading key store:"+e);
        }
        try {
            keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, pwd.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
            ServerSocketFactory factory = sslContext.getServerSocketFactory();
            staticServer.m_secureServerSocket = factory.createServerSocket(portNum);

        }
        catch (Exception e)
        {
            System.out.println("error when launch server:"+e);
            System.out.println(System.getProperty("user.dir"));
        }
        if(secureRequestThread == null)
        {
            secureRequestThread = new Thread(Server::secureServerLoop);
            secureRequestThread.start();
        }
    }

    public static void run() {
        ArrayList<RequestHandler> requestHandlers = new ArrayList<RequestHandler>();
        ArrayList<Socket> clientSocketList = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(100);

        while(true) {
            try {
                clientSocketList.add(staticServer.m_serverSocket.accept());
                requestHandlers.add(new RequestHandler(staticServer,
                        clientSocketList.get(clientSocketList.size()-1)));
                requestHandlers.get(requestHandlers.size()-1).isSecure = false;
                executor.execute(requestHandlers.get(requestHandlers.size()-1));
            } catch (Exception e) {
                System.out.println("into e "+e);
                throw new RuntimeException(e);
            }
        }
    }

    public static String matchGetKey(String pathStr, String host)
    {
        if(host == null)
            host = DEFAULT;
        if(!getHostMap.containsKey(host))
            host = DEFAULT;
        if(getHostMap.get(host) == null)
            return null;
        for(String key: getHostMap.get(host).keySet())
        {
            if(matchPattern(pathStr,key))
            {
                return key;
            }
        }
        return null;
    }

    public static String matchPutKey(String pathStr, String host)
    {
        if(host == null)
            host = DEFAULT;
        if(!putHostMap.containsKey(host))
            host = DEFAULT;
        for(String key: putHostMap.get(host).keySet())
        {
            if(matchPattern(pathStr,key))
            {
                return key;
            }
        }
        return null;
    }


    public static String matchPostKey(String pathStr, String host)
    {
        if(host == null)
            host = DEFAULT;
        if(!postHostMap.containsKey(host))
            host = DEFAULT;
        for(String key: postHostMap.get(host).keySet())
        {
            if(matchPattern(pathStr,key))
            {
                return key;
            }
        }
        return null;
    }

//    src is url, dst is the route registed in hashmap
    public static boolean matchPattern(String src, String dst)
    {
        String[] srcElements = src.split("/");
        String[] keyElements = dst.split("/");
        if (srcElements.length != keyElements.length) {
            return false;
        }
        for(int i=0;i< keyElements.length;i++)
        {
            if(!keyElements[i].startsWith(":") && !keyElements[i].equals(srcElements[i]))
            {
                return false;
            }
        }
        return true;
    }

    public static final String CRLF = "\r\n";
    static String Error304 = " Not Modified";
    static String Error400 = " Bad Request";
    static String Error403 = " Forbidden";
    static String Error404 = " Not Found";
    static String Error405 = " Bad Request";
    static String Error501 = " Not Allowed";
    static String Error505 = " Not Implemented";

    public static void main(String[] args) {
        String str1 = "/data/1702516153372_job-3.jar/becizarcritatmyeggjgzeyalokksevqfeggocom/value";
        String str2 = "/data/:table/:row/:column";
        System.out.println(matchPattern(str1,str2));
    }
}
