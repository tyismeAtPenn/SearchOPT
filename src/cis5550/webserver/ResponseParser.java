package cis5550.webserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Objects;

public class ResponseParser {
    // store the information of req args
    RequestParser m_requestParser;
    Response m_response;
    File m_resource;
    InputStream m_fileInputStream = null;
    int statusCode;

    ResponseParser(Response response, File resource)
    {
        m_response = response;
        m_resource = resource;
    }

    public static String getError(int sc)
    {
        switch (sc)
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

    void calculateStatusCode() throws FileNotFoundException {
        if(!m_requestParser.isGood || !m_requestParser.requestArgs.containsKey(RequestParser.HOST))
        {
            statusCode = 400;   // bad request
        }
        else if (!Objects.equals(m_requestParser.httpStr, "HTTP/1.1"))
        {
            statusCode = 505;   // http version not supported
        }
        else if (Objects.equals(m_requestParser.methodStr, "PUT") ||
                Objects.equals(m_requestParser.methodStr, "POST"))
        {
            statusCode = 405;
        }
        else if (!Objects.equals(m_requestParser.methodStr, "GET") &&
                !Objects.equals(m_requestParser.methodStr, "HEAD"))
        {
            statusCode = 501;
        }
        else if (m_resource.exists()) {
            if(m_resource.canRead() && !m_requestParser.haveDots())
            {
                m_fileInputStream = new FileInputStream(m_resource);
                statusCode = 200;
            }
            else
            {
                statusCode = 403;
            }
        }
        else
        {
            statusCode = 404;
        }
        m_response.status(statusCode,getError(statusCode));
    }

    static String Error304 = " Not Modified";
    static String Error400 = " Bad Request";
    static String Error403 = " Forbidden";
    static String Error404 = " Not Found";
    static String Error405 = " Bad Request";
    static String Error501 = " Not Allowed";
    static String Error505 = " Not Implemented";
}
