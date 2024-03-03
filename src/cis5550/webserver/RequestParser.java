package cis5550.webserver;

import java.io.File;
import java.text.*;
import java.util.*;


public class RequestParser {
    String methodStr;
    String resourceStr;
    String httpStr;
    String qParamStr;
    Map<String, String> requestArgs = new HashMap<String,String>();

    boolean isGood = true;

    // return true for success, false for failure
    boolean parse(String singleLine)
    {
        int i=0;
        for(;i<singleLine.length();i++)
            if(singleLine.charAt(i) == ':')
                break;
        // do this to prevent space in value string
        if(i<singleLine.length()-2)
            requestArgs.put(singleLine.substring(0,i).toLowerCase(),singleLine.substring(i+2));
        return true;
    }

    boolean haveDots()
    {
        String[] segments = resourceStr.split("/");
        for(String seg:segments)
        {
            if(Objects.equals(seg,".."))
            {
                return true;
            }
        }
        return false;
    }

    RequestParser(String[] lines) throws Exception {
        String[] firstLineElements = lines[0].split(" ");
        // METHOD $dir http_version
        if(firstLineElements.length != 3)
        {
            isGood = false;
            return;
        }
        else {
            methodStr = firstLineElements[0];
            resourceStr = firstLineElements[1];
            httpStr = firstLineElements[2];

            // resourceStr may contain ?
            String[] splitsRes = resourceStr.split("\\?");
            if(splitsRes.length>1) // >=2
            {
                resourceStr = splitsRes[0];
                qParamStr = splitsRes[1];
            }
        }
        for(int i=1;i<lines.length;i++)
        {
            if(!parse(lines[i]))
            {
                throw new Exception("not a valid argument line");
            }
        }

    }

    public static final String CONTENT_LENGTH = "content-length";
    public static final String HOST = "host";
    public static final String MODIFIEDSINCE = "if-modified-since";
    public static final String RANGE = "range";
}
