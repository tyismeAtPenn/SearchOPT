package cis5550.webserver;

import cis5550.tools.SNIInspector;

import java.awt.font.NumericShaper;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class RequestHandler implements Runnable{
    boolean isSecure = false;
    Server m_server;
    Socket m_clientSocket;
    Thread requestThread;
    RequestHandler(Server sever, Socket clientSocket)
    {
        m_server = sever;
        m_clientSocket = clientSocket;
    }

    public String parseHost(RequestParser requestParser) throws Exception {
        String host;
        host = requestParser.requestArgs.getOrDefault("host", Server.DEFAULT);
        // I don't know the port num after : have anything useful.
        host = host.split(":")[0];
        if(Objects.equals(host,"tyisme.cis5550.net"))
        {
            host = Server.DEFAULT;
        }
//        if(m_SNIInspector!=null)
//        {
////            m_SNIInspector.parseConnection(m_clientSocket);
////            m_SNIInspector.
//        }

        return host;
    }

    public SessionImpl parseSession(RequestParser requestParser,
                                           RequestImpl request)
    {
        SessionImpl session = null;
        String sessionRaw = requestParser.requestArgs.getOrDefault("cookie",null);

        if(sessionRaw != null)
        {
            // we need to parse it into several args:
            String[] subExprs = sessionRaw.split(";");
            for(String subExpr:subExprs)
            {
                if(subExpr.contains("="))
                {
                    String[] elements = subExpr.split("=");
                    elements[1] = elements[1].trim();
                    // delete the first and the last double qoute
                    if(elements[1].charAt(0) == '\"' &&
                        elements[1].charAt(elements[1].length()-1) == '\"')
                    {
                        elements[1] = elements[1].substring(1,elements[1].length()-1);
                    }
                    Server.staticServer.sessionArgs.put(elements[0].trim(),elements[1]);
                    if(Objects.equals(elements[0].trim(),"SessionID"))
                    {
                        session = Server.sessionMap.getOrDefault(elements[1].trim(),null);
                        if(session != null)
                        {
                            session.checkValidity();
                            if(session.isInvalid)
                            {
                                Server.sessionMap.remove(elements[1].trim());
                                session = null;
                                // TODO:and throw error:
                            }
                            else
                            {
                                System.out.println("get session:"+session.m_id);
                                request.currSession = session;
                            }
                        }
                    }
                }
                else if(Objects.equals(subExpr.trim().toLowerCase(),"httponly"))
                {
                    //TODO:
                    if(request.currSession != null)
                        request.currSession.couldRead = false;
                        System.out.println("httpOnly Open unreadable");
                }
                else if(Objects.equals(subExpr.trim().toLowerCase(),"secure"))
                {
                    //TODO:
                    if(!isSecure)
                        request.currSession = null;
                }
            }
        }
        if(session != null && session.attribute("samesite") != null)
        {
            if(session.attribute("samesite").toString().equals("Strict") &&
                Objects.equals(session.originalAddr,m_clientSocket.getRemoteSocketAddress()))
            {
                session.couldRead = false;
            }
        }
        return session;
    }

    public static HashMap<String,String> parseParam(String pattern,String path)
    {
        HashMap<String,String> retMap = new HashMap<>();
        String[] patternEle = pattern.split("/");
        String[] pathEle = path.split("/");
        for(int i=0;i<pathEle.length;i++)
        {
            if(!patternEle[i].isEmpty() &&patternEle[i].charAt(0) == ':')
            {
                retMap.put(patternEle[i].substring(1),pathEle[i]);
            }
        }
        return retMap;
    }

    public static HashMap<String,String> parseQParam(String path, String body)
    {
        HashMap<String,String> retMap = new HashMap<>();
        String queries = "";
        if(path != null)
        {
            queries += URLDecoder.decode(path);
        }
        // when there are Qparams:
        if(body != null)
        {
            System.out.println(URLDecoder.decode(body));
            queries += "&"+URLDecoder.decode(body);
        }
        if(queries.isEmpty())
            return  retMap;
        String[] params = queries.split("&");

        for(String param:params)
        {
            String[] entry = param.split("=");
            retMap.put(entry[0],entry[1]);
        }
        return retMap;
    }

    public static String[] readAndParse(InputStream in,
                                        Socket clientSocket) throws IOException {
//        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(in));
        char[] byteBuffer = new char[100000];
        String currentBufferStr = "";

        int size = 0;
        boolean isEnd = false;
        int tempChar = in.read();
        if(tempChar == -1)
        {
            clientSocket.close();
            return null;
        }
        else
        {
            byteBuffer[size++] = (char) tempChar;
        }
        while(!isEnd)
        {
            tempChar = (char)in.read();
            byteBuffer[size++] = (char)tempChar;

            currentBufferStr = new String(byteBuffer);
            if(size >= 4)// >=5
            {
                String last4Str = currentBufferStr.substring(size-4,size);
                if(Objects.equals(last4Str, CRLF + CRLF))
                {
                    isEnd = true;
                }
            }
        }
        return currentBufferStr.substring(0,size).split(CRLF);
    }

    byte[] readBody(InputStream in, int contentLen) throws IOException {
        byte[] byteBuffer = new byte[contentLen];
        int i = 0;

        for(;i<contentLen;i++)
        {
            byteBuffer[i] = (byte) in.read();
        }
        return byteBuffer;
        // TODO: is it possible that some packets comes later
        // So we need a timeout setter and monitor?
    }


    public void sendResponse(OutputStream out,
                                    Route route,
                                    RequestImpl request,
                                    ResponseImpl response
                                    ) throws Exception {
        if(request != null)
        {
            if(Server.beforeRoute != null)
                Server.beforeRoute.handle(request,response);
            Object returnStr = null;

            if(!response.hasClosed) // to prevent halt halfway
            {
                returnStr = route.handle(request,response);
                if(request.currSession != null & request.createdSession)
                {
                    request.currSession.originalAddr = m_clientSocket.getRemoteSocketAddress();
                    Server.sessionMap.put(request.currSession.id(),request.currSession);
                    response.header("Set-Cookie","SessionID="+request.currSession.id());
                    request.createdSession = false;
                }
            }
            if(Server.afterRoute != null && !response.hasClosed)
                Server.afterRoute.handle(request,response);
            if(returnStr != null)
            {
                response.write(returnStr.toString().getBytes());
            }
        }
        else
        {
            System.out.println("request is null");
        }
        if(!response.hasCommited)
        {
            out.write(response.getHeaderContent().getBytes());
        }
        if(response.bodyRaw != null)
        {
//            System.out.println("before body write write "+new String(response.bodyRaw,StandardCharsets.UTF_8));
            out.write(response.bodyRaw);
//            System.out.println("end body write");
        }
    }

    public void internalErrorHandle(ResponseImpl response,OutputStream out) throws Exception {
        response.status(500,"Internal Server Error");
        sendResponse(out,
                null,
                null,
                response
        );
    }

    @Override
    public void run() {
        try {
            InputStream in = m_clientSocket.getInputStream();
            while(true) {
                String[] lines = readAndParse(in,m_clientSocket);
                // if null indicate no further connection
                if(lines == null)
                    break;
                RequestParser requestParser = new RequestParser(lines);

                int contentLen = Integer.parseInt(requestParser.requestArgs.getOrDefault(RequestParser.CONTENT_LENGTH,"0"));
                byte[] bodyContent = null;
                OutputStream out = m_clientSocket.getOutputStream();
                ResponseImpl response = new ResponseImpl(m_server,out);

                // parse host:
                String host;
                host = parseHost(requestParser);
                if(host == null ||
                (Server.putHostMap.get(host) == null &&
                Server.getHostMap.get(host) == null &&
                Server.postHostMap.get(host) == null))
                    host = Server.DEFAULT;

                // parse session:

                if(contentLen > 0)
                {
                    bodyContent = readBody(in,contentLen);
//                    System.out.println("the body content is:"+new String(bodyContent));
                }

                if(Objects.equals(requestParser.methodStr,"GET") &&
                    Server.matchGetKey(requestParser.resourceStr, host) != null)
                {
//                    System.out.println("recv get:"+requestParser.resourceStr);
                    String key = Server.matchGetKey(requestParser.resourceStr, host);
                    try {
                        Route route =  Server.getHostMap.get(host).get(key);
                        RequestImpl request = handleRequest(requestParser, bodyContent, key);
                        parseSession(requestParser,request);
                        sendResponse(out,route,request,response);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        if(response.hasCommited)
                        {
                            m_clientSocket.close();
                            return;// end this thread
                        }
                        internalErrorHandle(response,out);
                    }
                }
                else if(Objects.equals(requestParser.methodStr,"POST") &&
                        Server.matchPostKey(requestParser.resourceStr, host) != null)
                {
                    String key = Server.matchPostKey(requestParser.resourceStr, host);
                    try {
//                        System.out.println("find in post:"+requestParser.resourceStr);
                        Route route =  Server.postHostMap.get(host).get(key);
                        RequestImpl request = handleRequest(requestParser, bodyContent, key);
                        parseSession(requestParser,request);
                        sendResponse(out,route,request,response);

                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        if(response.hasCommited)
                        {
                            m_clientSocket.close();
                            return;// end this thread
                        }
                        internalErrorHandle(response,out);
                        System.out.println("send error:"+new String(response.bodyRaw,StandardCharsets.UTF_8));
                    }
                }
                else if(Objects.equals(requestParser.methodStr,"PUT") &&
                        Server.matchPutKey(requestParser.resourceStr,host) != null)
                {
//                    System.out.println("find in put:"+requestParser.resourceStr);
                    String key = Server.matchPutKey(requestParser.resourceStr, host);
                    try {
                        Route route = Server.putHostMap.get(host).get(key);
                        RequestImpl request = handleRequest(requestParser, bodyContent, key);
                        parseSession(requestParser,request);
                        sendResponse(out,route,request,response);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        if(response.hasCommited)
                        {
                            m_clientSocket.close();
                            return;// end this thread
                        }
                        internalErrorHandle(response,out);
                    }
                }
                else
                {
                    // STATIC file reading:
                    System.out.println("not contain:"+requestParser.resourceStr+" host is :"+host);
                    System.out.println("put map:"+ Server.putHostMap);

                    File resource = new File(m_server.m_returnDir.get(host) + requestParser.resourceStr);

                    ResponseParser responseParser = new ResponseParser(response,resource);
                    responseParser.m_requestParser = requestParser;

                    responseParser.calculateStatusCode();
                    if(Objects.equals(requestParser.methodStr, "GET") && responseParser.statusCode != 304)
                    {
                        m_server.sendResponse(out,
                                responseParser.statusCode,
                                "text/plain",
                                resource,
                                responseParser.m_fileInputStream,
                                false);
                    }
                    else
                    {
                        m_server.sendResponse(out,
                                responseParser.statusCode,
                                "text/plain",
                                resource,
                                responseParser.m_fileInputStream,
                                true);
                    }
                }
            }
        }catch (IOException e) {
//            e.printStackTrace();
            //            System.out.println(tempChar);
//                System.out.println(e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public RequestImpl handleRequest(RequestParser requestParser, byte[] bodyContent, String key) {
        HashMap<String,String> param = parseParam(key, requestParser.resourceStr);
        HashMap<String,String> qParam;
        if(requestParser.requestArgs.containsKey("content-type")&&
            Objects.equals(requestParser.requestArgs.get("content-type"),"application/x-www-form-urlencoded"))
        {
            qParam = parseQParam(requestParser.qParamStr,new String(bodyContent));
        }
        else
        {
            qParam = parseQParam(requestParser.qParamStr,null);
        }

        return new RequestImpl(requestParser.methodStr,
                                        requestParser.resourceStr,
                                        requestParser.httpStr,
                                        requestParser.requestArgs,
                                        qParam,
                                        param,
                                        (InetSocketAddress)m_clientSocket.getRemoteSocketAddress(),
                                        bodyContent,
                                        m_server
                                        );
    }

    public void start()
    {
        if(requestThread == null)
        {
            requestThread = new Thread(this);
            requestThread.start();
        }
        else
        {
            System.out.println("Thread already start!");
        }
    }

    public static final String CRLF = "\r\n";


}
